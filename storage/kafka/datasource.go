package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/streaming/storage"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   {},
	physical.Secondary: {},
}

type DataSource struct {
	brokers      []string
	topic        string
	partition    int
	batchSize    int
	decodeAsJSON bool
	alias        string
	stateStorage storage.Storage
}

func NewDataSourceBuilderFactory(partitions int) physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partition int) (execution.Node, error) {
			// Get execution configuration
			hosts, ports, err := config.GetIPAddressList(dbConfig, "brokers", config.WithDefault([]interface{}{[]string{"localhost"}, []int{9092}}))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get address")
			}
			topic, err := config.GetString(dbConfig, "topic")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get topic")
			}
			batchSize, err := config.GetInt(dbConfig, "batchSize", config.WithDefault(1))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get batch size")
			}
			decodeAsJSON, err := config.GetBool(dbConfig, "json", config.WithDefault(false))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get json option")
			}

			brokers := make([]string, len(hosts))
			for i := range hosts {
				brokers[i] = fmt.Sprintf("%s:%d", hosts[i], ports[i])
			}

			return &DataSource{
				brokers:      brokers,
				topic:        topic,
				partition:    partition,
				batchSize:    batchSize,
				decodeAsJSON: decodeAsJSON,
				alias:        alias,
				stateStorage: matCtx.Storage,
			}, nil
		},
		nil,
		availableFilters,
		metadata.Unbounded,
		partitions,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	partitions, err := config.GetInt(dbConfig, "partitions", config.WithDefault(1))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get primaryKeys")
	}

	return NewDataSourceBuilderFactory(partitions), nil
}

var offsetPrefix = []byte("$kafka_offset$")

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix())

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   ds.brokers,
		Topic:     ds.topic,
		Dialer:    dialer,
		Partition: ds.partition,
		MinBytes:  10e1,
		MaxBytes:  10e7,
	})

	rs := &RecordStream{
		stateStorage: ds.stateStorage,
		streamID:     streamID,
		kafkaReader:  r,
		batchSize:    ds.batchSize,
		decodeAsJSON: ds.decodeAsJSON,
		alias:        ds.alias,
	}
	err := rs.loadOffset(tx)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't load kafka partition %d offset", ds.partition)
	}

	go rs.RunWorker(ctx)

	return rs, nil
}

type RecordStream struct {
	stateStorage storage.Storage
	streamID     *execution.StreamID
	kafkaReader  *kafka.Reader
	batchSize    int
	decodeAsJSON bool
	alias        string

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

var outputQueuePrefix = []byte("$output_queue$")

func (rs *RecordStream) RunWorker(ctx context.Context) {
	for {
		if err := rs.RunWorkerInternal(ctx); err != nil {
			log.Printf("error running kafka read messages worker: %s, reinitializing from storage", err.Error())
			tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())
			if err := rs.loadOffset(tx); err != nil {
				log.Fatalf("couldn't reinitialize offset for kafka read messages worker: %s", err)
				return
			}
			tx.Abort() // We only read data above, no need to risk failing now.
		}
	}
}

func (rs *RecordStream) RunWorkerInternal(ctx context.Context) error {
	tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())
	defer tx.Abort()
	outputQueue := execution.NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	// TODO: Remove
	time.Sleep(time.Second)

	batch := make([]*execution.Record, rs.batchSize)
	for i := 0; i < rs.batchSize; i++ {
		msg, err := rs.kafkaReader.ReadMessage(ctx)
		for err != nil {
			// Check if recoverable error.
			retryDuration := 5 * time.Second
			log.Printf("kafka read message error %s, retrying after %s", err.Error(), retryDuration)
			msg, err = rs.kafkaReader.ReadMessage(ctx)
		}

		fields := []octosql.VariableName{octosql.NewVariableName(fmt.Sprintf("%s.key", rs.alias))}
		values := []octosql.Value{octosql.MakeString(string(msg.Key))}
		if !rs.decodeAsJSON {
			fields = append(fields, octosql.NewVariableName(fmt.Sprintf("%s.value", rs.alias)))
			values = append(values, octosql.MakeString(string(msg.Value)))
		} else {
			object := make(map[string]interface{})
			err := json.Unmarshal(msg.Value, &object)
			if err != nil {
				log.Printf("couldn't decode value as json: %s, passing along as raw bytes", err)
				fields = append(fields, octosql.NewVariableName("value"))
				values = append(values, octosql.MakeString(string(msg.Value)))
			} else {
				for k, v := range object {
					fields = append(fields, octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, k)))
					values = append(values, octosql.NormalizeType(v))
				}
			}
		}
		batch[i] = execution.NewRecordFromSlice(fields, values)
	}

	for i := range batch {
		err := outputQueue.Push(ctx, &QueueElement{
			Type: &QueueElement_Record{
				Record: batch[i],
			},
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't push kafka message with index %d in batch to output record queue", i)
		}
	}

	if err := rs.saveOffset(tx); err != nil {
		return errors.Wrap(err, "couldn't save kafka offset")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "couldn't commit kafka read transaction")
	}

	return nil
}

func (rs *RecordStream) loadOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return errors.Wrap(err, "couldn't load kafka partition offset from state storage")
	} else {
		err := rs.kafkaReader.SetOffset(int64(offset.AsInt()))
		if err != nil {
			return errors.Wrap(err, "couldn't set initial kafka offset based on state storage")
		}
	}

	return nil
}

func (rs *RecordStream) saveOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	offset := octosql.MakeInt(int(rs.kafkaReader.Offset()))
	err := offsetState.Set(&offset)
	if err != nil {
		return errors.Wrap(err, "couldn't save kafka partition offset to state storage")
	}

	return nil
}

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(rs.streamID.AsPrefix())
	outputQueue := execution.NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	var queueElement QueueElement
	err := outputQueue.Pop(ctx, &queueElement)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't pop queue element")
	}
	switch queueElement := queueElement.Type.(type) {
	case *QueueElement_Record:
		return queueElement.Record, nil
	case *QueueElement_Error:
		return nil, errors.New(queueElement.Error)
	default:
		panic("invalid queue element type")
	}
}

func (rs *RecordStream) Close() error {
	rs.workerCtxCancel()
	err := <-rs.workerCloseErrChan
	if err != nil {
		return errors.Wrap(err, "couldn't stop worker")
	}

	return nil
}

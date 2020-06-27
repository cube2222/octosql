package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/storage"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   {},
	physical.Secondary: {},
}

type DataSource struct {
	brokers      []string
	topic        string
	partition    int
	startOffset  int
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
			startOffset, err := config.GetInt(dbConfig, "startOffset", config.WithDefault(-1))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get start offset")
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
				startOffset:  startOffset,
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
		return nil, errors.Wrap(err, "couldn't get partitions")
	}

	return NewDataSourceBuilderFactory(partitions), nil
}

var offsetPrefix = []byte("$kafka_offset$")
var tokenQueuePrefix = []byte("$token_queue$")

const initialTokenCount = 5

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix())

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	kafkaConfig := kafka.ReaderConfig{
		Brokers:   ds.brokers,
		Topic:     ds.topic,
		Dialer:    dialer,
		Partition: ds.partition,
		MinBytes:  10e1,
		MaxBytes:  10e7,
	}

	r := kafka.NewReader(kafkaConfig)

	rs := &RecordStream{
		stateStorage: ds.stateStorage,
		streamID:     streamID,
		kafkaReader:  r,
		startOffset:  ds.startOffset,
		batchSize:    ds.batchSize,
		decodeAsJSON: ds.decodeAsJSON,
		alias:        ds.alias,
	}
	err := rs.loadOffset(tx)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load kafka partition %d offset", ds.partition)
	}

	tokenQueue := execution.NewOutputQueue(tx.WithPrefix(tokenQueuePrefix))
	for i := 0; i < initialTokenCount; i++ {
		token := octosql.MakePhantom()
		err := tokenQueue.Push(ctx, &token)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't push batch token to queue")
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	rs.workerCtxCancel = cancel
	rs.workerCloseErrChan = make(chan error, 1)

	return rs,
		execution.NewExecutionOutput(
			execution.NewZeroWatermarkGenerator(),
			map[string]execution.ShuffleData{},
			[]execution.Task{func() error {
				err := rs.RunWorker(ctx)
				if err == context.Canceled || err == context.DeadlineExceeded {
					rs.workerCloseErrChan <- err
					return nil
				} else {
					err := errors.Wrap(err, "kafka worker error")
					rs.workerCloseErrChan <- err
					return err
				}
			}},
		),
		nil
}

type RecordStream struct {
	stateStorage storage.Storage
	streamID     *execution.StreamID
	kafkaReader  *kafka.Reader
	startOffset  int
	batchSize    int
	decodeAsJSON bool
	alias        string

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

var outputQueuePrefix = []byte("$output_queue$")

func (rs *RecordStream) RunWorker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())

		err := rs.RunWorkerInternal(ctx, tx)
		if errors.Cause(err) == execution.ErrNewTransactionRequired {
			tx.Abort()
			continue
		} else if waitableError := execution.GetErrWaitForChanges(err); waitableError != nil {
			tx.Abort()
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("kafka worker: couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("kafka worker: couldn't close storage changes subscription: ", err)
			}
			continue
		} else if err != nil {
			tx.Abort()
			log.Printf("kafka worker: error running kafka read messages worker: %s, reinitializing from storage", err)
			tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())
			if err := rs.loadOffset(tx); err != nil {
				return errors.Wrap(err, "couldn't reinitialize offset for kafka read messages worker")
			}
			tx.Abort() // We only read data above, no need to risk failing now.
			continue
		}

		err = tx.Commit()
		if err != nil {
			log.Println("kafka worker: couldn't commit transaction: ", err)
			continue
		}
	}
}

func (rs *RecordStream) RunWorkerInternal(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := execution.NewOutputQueue(tx.WithPrefix(outputQueuePrefix))
	tokenQueue := execution.NewOutputQueue(tx.WithPrefix(tokenQueuePrefix))

	// The worker needs a token to read a batch.
	var token octosql.Value
	err := tokenQueue.Pop(ctx, &token)
	if err != nil {
		return errors.Wrap(err, "couldn't get batch token from token queue")
	}

	batch := make([]*execution.Record, rs.batchSize)
	for i := 0; i < rs.batchSize; i++ {
		msg, err := rs.kafkaReader.ReadMessage(ctx)
		if err != nil {
			return errors.Wrap(err, "couldn't read message from kafka")
		}

		fields := []octosql.VariableName{
			octosql.NewVariableName(fmt.Sprintf("%s.key", rs.alias)),
			octosql.NewVariableName(fmt.Sprintf("%s.offset", rs.alias)),
		}
		values := []octosql.Value{
			octosql.MakeString(string(msg.Key)),
			octosql.MakeInt(int(msg.Offset)),
		}

		if !rs.decodeAsJSON {
			fields = append(fields, octosql.NewVariableName(fmt.Sprintf("%s.value", rs.alias)))
			values = append(values, octosql.MakeString(string(msg.Value)))
		} else {
			object := make(map[string]interface{})
			err := json.Unmarshal(msg.Value, &object)
			if err != nil {
				log.Printf("couldn't decode value as json: %s, passing along as raw bytes", err)
				fields = append(fields, octosql.NewVariableName(fmt.Sprintf("%s.value", rs.alias)))
				values = append(values, octosql.MakeString(string(msg.Value)))
			} else {
				keys := make([]string, 0, len(object))
				for k := range object {
					keys = append(keys, k)
				}
				sort.Slice(keys, func(i, j int) bool {
					return keys[i] < keys[j]
				})
				for _, k := range keys {
					fields = append(fields, octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, k)))
					values = append(values, octosql.NormalizeType(object[k]))
				}
			}
		}
		batch[i] = execution.NewRecordFromSlice(
			fields,
			values,
			execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(rs.streamID, int(msg.Offset))),
		)
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

	return nil
}

func (rs *RecordStream) loadOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
		if rs.startOffset != -1 {
			err := rs.kafkaReader.SetOffset(int64(rs.startOffset))
			if err != nil {
				return errors.Wrap(err, "couldn't set initial kafka offset based on start offset")
			}
		}
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

var readMessagesCountPrefix = []byte("$read_count$")

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(rs.streamID.AsPrefix())
	outputQueue := execution.NewOutputQueue(tx.WithPrefix(outputQueuePrefix))
	readCount := storage.NewValueState(tx.WithPrefix(readMessagesCountPrefix))
	tokenQueue := execution.NewOutputQueue(tx.WithPrefix(tokenQueuePrefix))

	var queueElement QueueElement
	err := outputQueue.Pop(ctx, &queueElement)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't pop queue element")
	}
	switch queueElement := queueElement.Type.(type) {
	case *QueueElement_Record:
		var count octosql.Value
		err := readCount.Get(&count)
		if err == storage.ErrNotFound {
			count = octosql.MakeInt(0)
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't read current read messages count")
		}

		newCount := count.AsInt() + 1

		// If we're done with a batch, send back a token.
		if newCount%rs.batchSize == 0 {
			newCount = 0
			token := octosql.MakePhantom()
			err := tokenQueue.Push(ctx, &token)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't push batch token to queue")
			}
		}

		count = octosql.MakeInt(newCount)
		err = readCount.Set(&count)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't save current read messages count")
		}

		return queueElement.Record, nil
	case *QueueElement_Error:
		return nil, errors.New(queueElement.Error)
	default:
		panic("invalid queue element type")
	}
}

func (rs *RecordStream) Close(ctx context.Context, storage storage.Storage) error {
	rs.workerCtxCancel()
	err := <-rs.workerCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop kafka worker")
	}

	if err := rs.kafkaReader.Close(); err != nil {
		return errors.Wrap(err, "couldn't close underlying kafka reader")
	}

	if err := storage.DropAll(rs.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

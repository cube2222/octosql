package kafka

import (
	"context"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"

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
	alias        string
	stateStorage storage.Storage
}

func NewDataSourceBuilderFactory(partitions int) physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partition int) (execution.Node, error) {
			// Get execution configuration
			hosts, ports, err := config.GetIPAddressList(dbConfig, "brokers", config.WithDefault([]interface{}{[]interface{}{"localhost"}, []interface{}{9092}}))
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

			brokers := make([]string, len(hosts))
			for i := range hosts {
				brokers[i] = fmt.Sprintf("%s:%d", hosts[i], ports[i])
			}

			return &DataSource{
				brokers:      brokers,
				topic:        topic,
				partition:    partition,
				batchSize:    batchSize,
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
	offsetState := storage.NewValueState(tx)

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

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't load kafka partition offset from state storage")
	} else {
		err := r.SetOffset(int64(offset.AsInt()))
		if err != nil {
			return nil, errors.Wrap(err, "couldn't set initial kafka offset based on state storage")
		}
	}

	rs := &RecordStream{
		stateStorage: ds.stateStorage,
		streamID:     streamID,
		kafkaReader:  r,
		batchSize:    ds.batchSize,
		alias:        ds.alias,
	}
	go rs.RunWorker(ctx)

	return rs, nil
}

type RecordStream struct {
	stateStorage storage.Storage
	streamID     *execution.StreamID
	kafkaReader  *kafka.Reader
	batchSize    int
	alias        string

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

var outputQueuePrefix = []byte("$output_queue$")

func (rs *RecordStream) RunWorker(ctx context.Context) {

	// On error reload old offset
	for {
		tx := rs.stateStorage.BeginTransaction()
		for i := 0; i < rs.batchSize; i++ {
			msg, err := rs.kafkaReader.ReadMessage(ctx)
		}
	}

	panic("implement me")
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

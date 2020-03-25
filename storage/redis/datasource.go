package redis

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/streaming/storage"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary: {
		physical.In:    {},
		physical.Equal: {},
	},
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	client       *redis.Client
	keyFormula   KeyFormula
	alias        string
	dbKey        string
	batchSize    int
	stateStorage storage.Storage
}

// NewDataSourceBuilderFactory creates a new datasource builder factory for a redis database.
// dbKey is the name for hard-coded key alias used in future formulas for redis queries
func NewDataSourceBuilderFactory(dbKey string) physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partitions int) (execution.Node, error) {
			host, port, err := config.GetIPAddress(dbConfig, "address", config.WithDefault([]interface{}{"localhost", 6379}))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get address")
			}
			dbIndex, err := config.GetInt(dbConfig, "databaseIndex", config.WithDefault(0))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get database index")
			}
			password, err := config.GetString(dbConfig, "password", config.WithDefault("")) // TODO: Change to environment.
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get password")
			}
			batchSize, err := config.GetInt(dbConfig, "batchSize", config.WithDefault(1000))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get batch size")
			}

			client := redis.NewClient(
				&redis.Options{
					Addr:     fmt.Sprintf("%s:%d", host, port),
					Password: password,
					DB:       dbIndex,
				},
			)

			keyFormula, err := NewKeyFormula(filter, dbKey, alias, matCtx)
			if err != nil {
				return nil, errors.Errorf("couldn't create KeyFormula")
			}

			return &DataSource{
				client:       client,
				keyFormula:   keyFormula,
				alias:        alias,
				dbKey:        dbKey,
				batchSize:    batchSize,
				stateStorage: matCtx.Storage,
			}, nil
		},
		[]octosql.VariableName{
			octosql.NewVariableName(dbKey),
		},
		availableFilters,
		metadata.BoundedDoesntFitInLocalStorage,
		1,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	dbKey, err := config.GetString(dbConfig, "databaseKeyName", config.WithDefault("key"))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get database key name")
	}

	return NewDataSourceBuilderFactory(dbKey), nil
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix())

	keysWanted, err := ds.keyFormula.getAllKeys(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get all keys from filter")
	}

	rs := &RecordStream{
		stateStorage: ds.stateStorage,
		streamID:     streamID,
		client:       ds.client,
		alias:        ds.alias,
		keyName:      ds.dbKey,
		batchSize:    ds.batchSize,
	}
	if err = rs.loadOffset(tx); err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load redis offset")
	}
	if err = rs.loadCursor(tx); err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load redis cursor")
	}

	if len(keysWanted.keys) == 0 { // EntireDatabaseStream
		rs.isEntireDatabaseStream = true
	} else { // KeySpecificStream
		sliceKeys := make([]string, 0)
		for k := range keysWanted.keys {
			sliceKeys = append(sliceKeys, k)
		}
		rs.keys = sliceKeys
	}

	go func() {
		log.Println("worker start")
		rs.RunWorker(ctx)
		log.Println("worker done")
	}()

	return rs, execution.NewExecutionOutput(execution.NewZeroWatermarkGenerator()), nil
}

type RecordStream struct {
	stateStorage storage.Storage
	streamID     *execution.StreamID
	client       *redis.Client
	isDone       bool
	alias        string
	keyName      string
	offset       int
	batchSize    int

	isEntireDatabaseStream bool

	// Field used by EntireDatabaseStream
	cursor uint64 // cursor is the parameter for scan which indicates a place where next scan should continue

	// Fields used by KeySpecificStream
	keys    []string
	counter int
}

func (rs *RecordStream) Close() error {
	err := rs.client.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying client")
	}

	return nil
}

func (rs *RecordStream) RunWorker(ctx context.Context) {
	panic("implement me")
}

func (rs *RecordStream) RunWorkerInternal(ctx context.Context, tx storage.StateTransaction) error {
	panic("implement me")
}

func getNewRecord(client *redis.Client, keyName, key, alias string) (*execution.Record, error) {
	recordValues, err := client.HGetAll(key).Result()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could't get hash for key %s", key))
	}

	// We skip this record
	if len(recordValues) == 0 {
		return nil, ErrNotFound
	}

	keyVariableName := octosql.NewVariableName(fmt.Sprintf("%s.%s", alias, keyName))

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for k, v := range recordValues {
		fieldName := octosql.NewVariableName(fmt.Sprintf("%s.%s", alias, k))
		aliasedRecord[fieldName] = octosql.NormalizeType(v)
	}

	fieldNames := make([]octosql.VariableName, 0)
	fieldNames = append(fieldNames, keyVariableName)
	for k := range aliasedRecord {
		fieldNames = append(fieldNames, k)
	}

	aliasedRecord[keyVariableName] = octosql.NormalizeType(key)

	// The key is always the first record field
	sort.Slice(fieldNames[1:], func(i, j int) bool {
		return fieldNames[i+1] < fieldNames[j+1]
	})

	return execution.NewRecord(fieldNames, aliasedRecord), nil
}

var cursorPrefix = []byte("redis_cursor")

func (rs *RecordStream) loadCursor(tx storage.StateTransaction) error {
	cursorState := storage.NewValueState(tx.WithPrefix(cursorPrefix))

	var cursor octosql.Value
	err := cursorState.Get(&cursor)
	if err == storage.ErrNotFound {
		cursor = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't load redis cursor from state storage")
	}

	rs.cursor = uint64(cursor.AsInt())

	return nil
}

func (rs *RecordStream) saveCursor(tx storage.StateTransaction, cursor uint64) error {
	cursorState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	rs.cursor = cursor

	cursorValue := octosql.MakeInt(int(cursor))
	err := cursorState.Set(&cursorValue)
	if err != nil {
		return errors.Wrap(err, "couldn't save redis cursor to state storage")
	}

	return nil
}

var offsetPrefix = []byte("redis_offset")

func (rs *RecordStream) loadOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
		offset = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't load redis offset from state storage")
	}

	rs.offset = offset.AsInt()

	return nil
}

func (rs *RecordStream) saveOffset(tx storage.StateTransaction, curBatchSize int) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	rs.offset = rs.offset + curBatchSize

	offset := octosql.MakeInt(rs.offset)
	err := offsetState.Set(&offset)
	if err != nil {
		return errors.Wrap(err, "couldn't save redis offset to state storage")
	}

	return nil
}

var outputQueuePrefix = []byte("$output_queue$")

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
		if queueElement.Error == execution.ErrEndOfStream.Error() {
			return nil, execution.ErrEndOfStream
		}

		return nil, errors.New(queueElement.Error)
	default:
		panic("invalid queue element type")
	}
}

//func (rs *EntireDatabaseStream) Next(ctx context.Context) (*execution.Record, error) {
//	for {
//		if rs.isDone {
//			return nil, execution.ErrEndOfStream
//		}
//
//		if !rs.dbIterator.Next() {
//			if rs.dbIterator.Err() != nil {
//				return nil, rs.dbIterator.Err()
//			}
//			rs.isDone = true
//			return nil, execution.ErrEndOfStream
//		}
//		key := rs.dbIterator.Val()
//
//		record, err := getNewRecord(rs.client, rs.keyName, key, rs.alias)
//		if err != nil {
//			if err == ErrNotFound { // key was not in redis database so we skip it
//				continue
//			}
//			return nil, err
//		}
//
//		return record, nil
//	}
//}

//func (rs *KeySpecificStream) Next(ctx context.Context) (*execution.Record, error) {
//	if rs.isDone {
//		return nil, execution.ErrEndOfStream
//	}
//
//	if rs.counter == len(rs.keys) {
//		rs.isDone = true
//		return nil, execution.ErrEndOfStream
//	}
//
//	key := rs.keys[rs.counter]
//	rs.counter++
//
//	return getNewRecord(rs.client, rs.keyName, key, rs.alias)
//}

var ErrNotFound = errors.New("redis key not found")

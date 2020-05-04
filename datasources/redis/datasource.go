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
	"github.com/cube2222/octosql/storage"
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

	if len(keysWanted.keys) == 0 { // EntireDatabaseStream
		rs.isEntireDatabaseStream = true
	} else { // KeySpecificStream
		rs.keys = make([]string, 0)
		for k := range keysWanted.keys {
			rs.keys = append(rs.keys, k)
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
					err := errors.Wrap(err, "redis worker error")
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
	client       *redis.Client
	isDone       bool
	alias        string
	keyName      string
	offset       int
	batchSize    int

	isEntireDatabaseStream bool

	// Field used by EntireDatabaseStream
	cursor uint64 // cursor is the parameter for scan which indicates a place where next scan should continue

	// Field used by KeySpecificStream
	keys []string

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

func (rs *RecordStream) Close(ctx context.Context, storage storage.Storage) error {
	rs.workerCtxCancel()
	err := <-rs.workerCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop redis worker")
	}

	if err := rs.client.Close(); err != nil {
		return errors.Wrap(err, "couldn't close underlying redis client")
	}

	if err := storage.DropAll(rs.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

func (rs *RecordStream) RunWorker(ctx context.Context) error {
	for { // outer for is loading offset value and moving file iterator
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())

		if err := rs.loadOffset(tx); err != nil {
			return errors.Wrap(err, "couldn't reinitialize offset for redis read batch worker")
		}

		// Moving file iterator by `rs.offset`
		if rs.isEntireDatabaseStream {
			// Here we do full database scan so all we need to do is load last returned cursor value
			if err := rs.loadCursor(tx); err != nil {
				return errors.Wrap(err, "couldn't load redis cursor")
			}
		} else {
			// Here we just drop first `rs.offset` from keys slice
			rs.keys = rs.keys[rs.offset:]
		}

		tx.Abort() // We only read data above, no need to risk failing now.

		for { // inner for is calling RunWorkerInternal
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
					log.Println("redis worker: couldn't listen for changes: ", err)
				}
				err = waitableError.Close()
				if err != nil {
					log.Println("redis worker: couldn't close storage changes subscription: ", err)
				}
				continue
			} else if err == execution.ErrEndOfStream {
				err = tx.Commit()
				if err != nil {
					log.Println("redis worker: couldn't commit transaction: ", err)
					continue
				}
				return ctx.Err()
			} else if err != nil {
				tx.Abort()
				log.Printf("redis worker: error running redis read batch worker: %s, reinitializing from storage", err)
				break
			}

			err = tx.Commit()
			if err != nil {
				log.Println("redis worker: couldn't commit transaction: ", err)
				continue
			}
		}
	}
}

var outputQueuePrefix = []byte("$output_queue$")

func (rs *RecordStream) RunWorkerInternal(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := execution.NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	if rs.isDone {
		err := outputQueue.Push(ctx, &QueueElement{
			Type: &QueueElement_EndOfStream{
				EndOfStream: true,
			},
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't push csv EndOfStream to output record queue")
		}

		return execution.ErrEndOfStream
	}

	var allKeysForThisBatch []string
	var newCursor uint64
	var err error

	if rs.isEntireDatabaseStream {
		// Performing scan from cursor -> extracting `rs.batchSize` new elements
		allKeysForThisBatch, newCursor, err = rs.client.Scan(rs.cursor, "*", int64(rs.batchSize)).Result()
		if err != nil {
			return errors.Wrapf(err, "couldn't scan new batch for redis worker")
		}
	} else {
		// Taking first `rs.batchSize` elements from all keys needed
		if len(rs.keys) < rs.batchSize {
			allKeysForThisBatch = rs.keys
		} else {
			allKeysForThisBatch = rs.keys[:rs.batchSize]
		}
	}

	batch := make([]*execution.Record, 0)
	for i := 0; i < len(allKeysForThisBatch); i++ {
		key := allKeysForThisBatch[i]
		recordValues, err := rs.client.HGetAll(key).Result()
		if err != nil {
			return errors.Wrapf(err, "could't get hash for key %s", key)
		}

		// We skip this record
		if len(recordValues) == 0 {
			return errors.New("redis key not found")
		}

		keyVariableName := octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, rs.keyName))

		aliasedRecord := make(map[octosql.VariableName]octosql.Value)
		for k, v := range recordValues {
			fieldName := octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, k))
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

		batch = append(batch, execution.NewRecord(
			fieldNames,
			aliasedRecord,
			execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(rs.streamID, rs.offset+i))))
	}

	for i := range batch {
		err := outputQueue.Push(ctx, &QueueElement{
			Type: &QueueElement_Record{
				Record: batch[i],
			},
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't push redis record with index %d in batch to output record queue", i)
		}
	}

	rs.offset = rs.offset + len(batch)
	if err := rs.saveOffset(tx); err != nil {
		return errors.Wrap(err, "couldn't save redis offset")
	}

	if rs.isEntireDatabaseStream {
		rs.cursor = newCursor
		if err := rs.saveCursor(tx); err != nil {
			return errors.Wrap(err, "couldn't save redis cursor")
		}

		if newCursor == 0 { // Whole database scanned
			rs.isDone = true
		}
	} else {
		rs.keys = rs.keys[len(batch):]

		if len(rs.keys) == 0 { // All keys handled
			rs.isDone = true
		}
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

func (rs *RecordStream) saveOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	offset := octosql.MakeInt(rs.offset)
	err := offsetState.Set(&offset)
	if err != nil {
		return errors.Wrap(err, "couldn't save redis offset to state storage")
	}

	return nil
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

func (rs *RecordStream) saveCursor(tx storage.StateTransaction) error {
	cursorState := storage.NewValueState(tx.WithPrefix(cursorPrefix))

	cursorValue := octosql.MakeInt(int(rs.cursor))
	err := cursorState.Set(&cursorValue)
	if err != nil {
		return errors.Wrap(err, "couldn't save redis cursor to state storage")
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
	case *QueueElement_EndOfStream:
		return nil, execution.ErrEndOfStream
	case *QueueElement_Error:
		return nil, errors.New(queueElement.Error)
	default:
		panic("invalid queue element type")
	}
}

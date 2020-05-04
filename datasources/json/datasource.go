package json

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/storage"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path         string
	alias        string
	arrayFormat  bool
	batchSize    int
	stateStorage storage.Storage
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partition int) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}
			arrayFormat, err := config.GetBool(dbConfig, "arrayFormat", config.WithDefault(false))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get if json in array form")
			}
			batchSize, err := config.GetInt(dbConfig, "batchSize", config.WithDefault(1000))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get batch size")
			}

			return &DataSource{
				path:         path,
				arrayFormat:  arrayFormat,
				alias:        alias,
				batchSize:    batchSize,
				stateStorage: matCtx.Storage,
			}, nil
		},
		nil,
		availableFilters,
		metadata.BoundedFitsInLocalStorage,
		1,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewDataSourceBuilderFactory(), nil
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	rs := &RecordStream{
		stateStorage:                  ds.stateStorage,
		streamID:                      streamID,
		arrayFormat:                   ds.arrayFormat,
		arrayFormatOpeningBracketRead: false,
		filePath:                      ds.path,
		isDone:                        false,
		alias:                         ds.alias,
		batchSize:                     ds.batchSize,
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
					err := errors.Wrap(err, "json worker error")
					rs.workerCloseErrChan <- err
					return err
				}
			}},
		),
		nil
}

type RecordStream struct {
	stateStorage                  storage.Storage
	streamID                      *execution.StreamID
	arrayFormat                   bool
	arrayFormatOpeningBracketRead bool
	filePath                      string
	file                          *os.File
	decoder                       *json.Decoder
	isDone                        bool
	alias                         string
	offset                        int
	batchSize                     int

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

func (rs *RecordStream) Close(ctx context.Context, storage storage.Storage) error {
	rs.workerCtxCancel()
	err := <-rs.workerCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop json worker")
	}

	if err := rs.file.Close(); err != nil {
		return errors.Wrap(err, "couldn't close underlying file")
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
			return errors.Wrap(err, "couldn't reinitialize offset for json read batch worker")
		}

		tx.Abort() // We only read data above, no need to risk failing now.

		// Load/Reload file
		file, err := os.Open(rs.filePath)
		if err != nil {
			return errors.Wrap(err, "couldn't open file")
		}

		rs.file = file
		rs.decoder = json.NewDecoder(file)

		// Moving file iterator by `rs.offset`
		for i := 0; i < rs.offset; i++ {
			_, err := rs.readRecordFromFile()
			if err == execution.ErrEndOfStream {
				return ctx.Err()
			} else if err != nil {
				return errors.Wrapf(err, "couldn't move json file iterator by %d offset", rs.offset)
			}
		}

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
					log.Println("json worker: couldn't listen for changes: ", err)
				}
				err = waitableError.Close()
				if err != nil {
					log.Println("json worker: couldn't close storage changes subscription: ", err)
				}
				continue
			} else if err == execution.ErrEndOfStream {
				err = tx.Commit()
				if err != nil {
					log.Println("json worker: couldn't commit transaction: ", err)
					continue
				}
				return ctx.Err()
			} else if err != nil {
				tx.Abort()
				log.Printf("json worker: error running json read batch worker: %s, reinitializing from storage", err)
				break
			}

			err = tx.Commit()
			if err != nil {
				log.Println("json worker: couldn't commit transaction: ", err)
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
			return errors.Wrapf(err, "couldn't push json EndOfStream to output record queue")
		}

		return execution.ErrEndOfStream
	}

	batch := make([]*execution.Record, 0)
	for i := 0; i < rs.batchSize; i++ {
		record, err := rs.readRecordFromFile()
		if err == execution.ErrEndOfStream {
			break
		} else if err != nil {
			return errors.Wrap(err, "couldn't read record from json file")
		}

		aliasedRecord := make(map[octosql.VariableName]octosql.Value)
		for k, v := range record {
			if str, ok := v.(string); ok {
				parsed, err := time.Parse(time.RFC3339, str)
				if err == nil {
					v = parsed
				}
			}
			aliasedRecord[octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, k))] = octosql.NormalizeType(v)
		}

		fields := make([]octosql.VariableName, 0)
		for k := range aliasedRecord {
			fields = append(fields, k)
		}

		sort.Slice(fields, func(i, j int) bool {
			return fields[i] < fields[j]
		})

		batch = append(batch, execution.NewRecord(
			fields,
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
			return errors.Wrapf(err, "couldn't push json record with index %d in batch to output record queue", i)
		}
	}

	rs.offset = rs.offset + len(batch)
	if err := rs.saveOffset(tx); err != nil {
		return errors.Wrap(err, "couldn't save json offset")
	}

	return nil
}

func (rs *RecordStream) readRecordFromFile() (map[octosql.VariableName]interface{}, error) {
	if rs.arrayFormat && !rs.arrayFormatOpeningBracketRead {
		tok, err := rs.decoder.Token() // Read opening [
		if tok != json.Delim('[') {
			return nil, errors.Errorf("expected [ as first json token, got %v", tok)
		}
		if err != nil {
			return nil, errors.Wrap(err, "couldn't read json opening bracket")
		}
		rs.arrayFormatOpeningBracketRead = true
	}

	if !rs.decoder.More() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	var record map[octosql.VariableName]interface{}
	err := rs.decoder.Decode(&record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't decode json record")
	}

	return record, nil
}

var offsetPrefix = []byte("json_offset")

func (rs *RecordStream) loadOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
		offset = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't load json offset from state storage")
	}

	rs.offset = offset.AsInt()

	return nil
}

func (rs *RecordStream) saveOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	offset := octosql.MakeInt(rs.offset)
	err := offsetState.Set(&offset)
	if err != nil {
		return errors.Wrap(err, "couldn't save json offset to state storage")
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

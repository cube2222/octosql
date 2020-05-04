package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/storage"
)

type SQLSourceTemplate interface {
	GetIPAddress(dbConfig map[string]interface{}) (string, int, error)
	GetDSNAndDriverName(user, password, host, dbName string, port int) (string, string)
	GetPlaceholders(alias string) PlaceholderMap
	GetAvailableFilters() map[physical.FieldType]map[physical.Relation]struct{}
}

type DataSource struct {
	db           *sql.DB
	stmt         *sql.Stmt
	placeholders []execution.Expression
	alias        string

	stateStorage storage.Storage
	batchSize    int
}

var (
	offsetPlaceholderName = octosql.VariableName("$0ffset_pl4ceholder$") // SURELY nobody is going to use it, right?

	// this is limit for postgres' bigint, mysql's is 2 times bigger but we want to fill the template pattern
	maxLimit = "9223372036854775807" // well, big thanks to mySQL for that!
)

func NewDataSourceBuilderFactoryFromTemplate(template SQLSourceTemplate) func([]octosql.VariableName) physical.DataSourceBuilderFactory {
	return func(primaryKeys []octosql.VariableName) physical.DataSourceBuilderFactory {
		return physical.NewDataSourceBuilderFactory(
			func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partitions int) (execution.Node, error) {
				// Get execution configuration
				host, port, err := template.GetIPAddress(dbConfig)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get address")
				}

				user, err := config.GetString(dbConfig, "user")
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get user")
				}

				databaseName, err := config.GetString(dbConfig, "databaseName")
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get databaseName")
				}

				tableName, err := config.GetString(dbConfig, "tableName")
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get tableName")
				}

				password, err := config.GetString(dbConfig, "password")
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get password")
				}

				batchSize, err := config.GetInt(dbConfig, "batchSize", config.WithDefault(1000))
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get batch size")
				}

				dsn, driver := template.GetDSNAndDriverName(user, password, host, databaseName, port)

				db, err := sql.Open(driver, dsn)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't connect to the database")
				}

				placeholders := template.GetPlaceholders(alias)

				// Create a query with placeholders to prepare a statement from a physical formula
				query := FormulaToSQL(filter, placeholders)

				// Adding Offset placeholder
				offsetPlaceholder := placeholders.AddPlaceholder(physical.NewVariable(offsetPlaceholderName))

				query = fmt.Sprintf("SELECT * FROM %s %s WHERE %s LIMIT %s OFFSET %s", tableName, alias, query, maxLimit, offsetPlaceholder)

				stmt, err := db.Prepare(query)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't prepare db for query")
				}

				// Materialize the created placeholders
				execAliases, err := placeholders.MaterializePlaceholders(matCtx)

				if err != nil {
					return nil, errors.Wrap(err, "couldn't materialize placeholders")
				}

				return &DataSource{
					stmt:         stmt,
					placeholders: execAliases,
					alias:        alias,
					db:           db,
					batchSize:    batchSize,
					stateStorage: matCtx.Storage,
				}, nil
			},
			primaryKeys,
			template.GetAvailableFilters(),
			metadata.BoundedDoesntFitInLocalStorage,
			1,
		)
	}
}

var offsetPrefix = []byte("sql_offset")

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	rs := &RecordStream{
		stateStorage: ds.stateStorage,
		streamID:     streamID,
		isDone:       false,
		alias:        ds.alias,
		stmt:         ds.stmt,
		variables:    variables,
		placeholders: ds.placeholders,
		batchSize:    ds.batchSize,
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
					err := errors.Wrap(err, "sql worker error")
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
	rows         *sql.Rows
	columns      []string
	isDone       bool
	alias        string

	stmt         *sql.Stmt
	variables    octosql.Variables
	placeholders []execution.Expression
	offset       int
	batchSize    int

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

func (rs *RecordStream) Close(ctx context.Context, storage storage.Storage) error {
	rs.workerCtxCancel()
	err := <-rs.workerCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop sql worker")
	}

	if err := rs.rows.Close(); err != nil {
		return errors.Wrap(err, "couldn't close underlying SQL rows")
	}

	if err := rs.stmt.Close(); err != nil {
		return errors.Wrap(err, "couldn't close underlying SQL stmt")
	}

	if err := storage.DropAll(rs.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

func (rs *RecordStream) RunWorker(ctx context.Context) error {
	for { // outer for is loading offset value and creating rows
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())

		err := rs.loadOffset(tx)
		if err != nil {
			return errors.Wrap(err, "couldn't reinitialize offset for sql read batch worker")
		}

		tx.Abort() // We only read data above, no need to risk failing now.

		// Adding offset value to variables
		newOffsetValue := octosql.MakeInt(rs.offset)

		offsetValue, err := rs.variables.Get(offsetPlaceholderName)
		if offsetValue.GetType() == octosql.TypeNull { // the variable doesn't exist, merge it with variables
			offsetVariable := octosql.NewVariables(map[octosql.VariableName]octosql.Value{offsetPlaceholderName: newOffsetValue})
			rs.variables, err = rs.variables.MergeWith(offsetVariable)
			if err != nil {
				return errors.Wrap(err, "couldn't merge variables with offset variable")
			}
		} else { // just update value in variables // TODO - maybe think about some Set() method on variables? MergeWith returns error when exists
			rs.variables[offsetPlaceholderName] = newOffsetValue
		}

		values := make([]interface{}, 0)

		for _, expression := range rs.placeholders {
			// Since we have an execution expression, then we can evaluate it given the variables
			value, err := expression.ExpressionValue(ctx, rs.variables)
			if err != nil {
				return errors.Wrap(err, "couldn't get actual value from variables")
			}

			values = append(values, value.ToRawValue())
		}

		rows, err := rs.stmt.QueryContext(ctx, values...)
		if err != nil {
			return errors.Wrap(err, "couldn't query statement")
		}
		rs.rows = rows

		columns, err := rows.Columns()
		if err != nil {
			return errors.Wrap(err, "couldn't get columns from rows")
		}
		rs.columns = columns

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
					log.Println("sql worker: couldn't listen for changes: ", err)
				}
				err = waitableError.Close()
				if err != nil {
					log.Println("sql worker: couldn't close storage changes subscription: ", err)
				}
				continue
			} else if err == execution.ErrEndOfStream {
				err = tx.Commit()
				if err != nil {
					log.Println("sql worker: couldn't commit transaction: ", err)
					continue
				}
				return ctx.Err()
			} else if err != nil {
				tx.Abort()
				log.Printf("sql worker: error running sql read batch worker: %s, reinitializing from storage", err)
				break
			}

			err = tx.Commit()
			if err != nil {
				log.Println("sql worker: couldn't commit transaction: ", err)
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
			return errors.Wrapf(err, "couldn't push sql EndOfStream to output record queue")
		}

		log.Println("sql worker: ErrEndOfStream pushed")
		return execution.ErrEndOfStream
	}

	batch := make([]*execution.Record, 0)
	for i := 0; i < rs.batchSize; i++ {
		if rs.isDone {
			break
		}

		if !rs.rows.Next() {
			rs.isDone = true
			break
		}

		cols := make([]interface{}, len(rs.columns))
		colPointers := make([]interface{}, len(cols))
		for i := range cols {
			colPointers[i] = &cols[i]
		}

		if err := rs.rows.Scan(colPointers...); err != nil {
			return errors.Wrap(err, "couldn't scan row")
		}

		resultMap := make(map[octosql.VariableName]octosql.Value)

		fields := make([]octosql.VariableName, len(rs.columns))
		for i, columnName := range rs.columns {
			newName := octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, columnName))
			fields[i] = newName

			// MySQL parses strings as []byte. We just assume strings are what we want really.
			if data, ok := cols[i].([]byte); ok {
				cols[i] = string(data)
			}

			resultMap[newName] = octosql.NormalizeType(cols[i])
		}

		batch = append(batch, execution.NewRecord(
			fields,
			resultMap,
			execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(rs.streamID, rs.offset+i))))
	}

	for i := range batch {
		err := outputQueue.Push(ctx, &QueueElement{
			Type: &QueueElement_Record{
				Record: batch[i],
			},
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't push sql record with index %d in batch to output record queue", i)
		}

		log.Println("sql worker: record pushed: ", batch[i])
	}

	rs.offset = rs.offset + len(batch)
	if err := rs.saveOffset(tx); err != nil {
		return errors.Wrap(err, "couldn't save sql offset")
	}

	return nil
}

func (rs *RecordStream) loadOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
		offset = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't load sql offset from state storage")
	}

	rs.offset = offset.AsInt()

	return nil
}

func (rs *RecordStream) saveOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	offset := octosql.MakeInt(rs.offset)
	err := offsetState.Set(&offset)
	if err != nil {
		return errors.Wrap(err, "couldn't save sql offset to state storage")
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

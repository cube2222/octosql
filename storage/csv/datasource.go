package csv

// only comma-separated now. Changing that means setting RecordStream.r.Coma
// .csv reader trims leading white space(s)

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"unicode/utf8"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/streaming/storage"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path           string
	alias          string
	hasColumnNames bool
	separator      rune
	batchSize      int
	stateStorage   storage.Storage
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partition int) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}
			hasColumns, err := config.GetBool(dbConfig, "headerRow", config.WithDefault(true))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get headerRow")
			}
			separator, err := config.GetString(dbConfig, "separator", config.WithDefault(","))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get separator")
			}
			r, _ := utf8.DecodeRune([]byte(separator))
			if r == utf8.RuneError {
				return nil, errors.Errorf("couldn't decode separator %s to rune", separator)
			}
			batchSize, err := config.GetInt(dbConfig, "batchSize", config.WithDefault(1000))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get batch size")
			}

			return &DataSource{
				path:           path,
				alias:          alias,
				hasColumnNames: hasColumns,
				separator:      r,
				batchSize:      batchSize,
				stateStorage:   matCtx.Storage,
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
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix())

	file, err := os.Open(ds.path)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't open file")
	}
	r := csv.NewReader(file)
	r.Comma = ds.separator
	r.TrimLeadingSpace = true

	rs := &RecordStream{
		stateStorage:    ds.stateStorage,
		streamID:        streamID,
		file:            file,
		r:               r,
		isDone:          false,
		alias:           ds.alias,
		first:           true,
		hasColumnHeader: ds.hasColumnNames,
		batchSize:       ds.batchSize,
	}
	if err = rs.loadOffset(tx); err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load csv offset")
	}

	ctx, cancel := context.WithCancel(ctx)
	rs.workerCtxCancel = cancel
	rs.workerCloseErrChan = make(chan error)

	go func() {
		log.Println("worker start")
		rs.RunWorker(ctx)
		log.Println("worker done")
	}()

	return rs, execution.NewExecutionOutput(execution.NewZeroWatermarkGenerator()), nil
}

type RecordStream struct {
	stateStorage    storage.Storage
	streamID        *execution.StreamID
	file            *os.File
	r               *csv.Reader
	isDone          bool
	alias           string
	aliasedFields   []octosql.VariableName
	first           bool
	hasColumnHeader bool
	offset          int
	batchSize       int

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

func (rs *RecordStream) Close() error {
	err := rs.file.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close underlying file")
	}

	return nil
}

var offsetPrefix = []byte("csv_offset")

func (rs *RecordStream) RunWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			rs.workerCloseErrChan <- ctx.Err()
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
				log.Println("csv worker: couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("csv worker: couldn't close storage changes subscription: ", err)
			}
			continue
		} else if err == execution.ErrEndOfStream {
			err = tx.Commit()
			if err != nil {
				log.Println("csv worker: couldn't commit transaction: ", err)
				continue
			}
			return
		} else if err != nil {
			tx.Abort()
			log.Printf("csv worker: error running csv read batch worker: %s, reinitializing from storage", err)
			tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())
			if err := rs.loadOffset(tx); err != nil {
				log.Fatalf("csv worker: couldn't reinitialize offset for csv worker: %s", err)
				return
			}
			tx.Abort() // We only read data above, no need to risk failing now.
			continue
		}

		err = tx.Commit()
		if err != nil {
			log.Println("csv worker: couldn't commit transaction: ", err)
			continue
		}
	}
}

var outputQueuePrefix = []byte("$output_queue$")

func (rs *RecordStream) RunWorkerInternal(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := execution.NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	if rs.isDone {
		err := outputQueue.Push(ctx, &QueueElement{
			Type: &QueueElement_Error{
				Error: execution.ErrEndOfStream.Error(),
			},
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't push csv EndOfStream to output record queue")
		}

		log.Println("csv worker: ErrEndOfStream pushed")
		return execution.ErrEndOfStream
	}

	batch := make([]*execution.Record, 0)

	if rs.first && rs.offset == 0 { // offset == 0 cause when we got crashed rs.first will be by default 0 but offset won't
		if rs.hasColumnHeader {
			err := rs.initializeColumnsWithHeaderRow()
			if err != nil {
				return errors.Wrap(err, "couldn't initialize columns for record stream")
			}
		} else {
			record, err := rs.initializeColumnsWithoutHeaderRow()
			if err != nil {
				return errors.Wrap(err, "couldn't initialize columns for record stream")
			}

			batch = append(batch, record)
		}
	}
	rs.first = false

	for i := len(batch); i < rs.batchSize; i++ {
		line, err := rs.r.Read()
		if err == io.EOF {
			rs.isDone = true
			rs.file.Close()
			break
		} else if err != nil {
			return errors.Wrap(err, "couldn't read record")
		}

		aliasedRecord := make(map[octosql.VariableName]octosql.Value)
		for i, v := range line {
			aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
		}

		batch = append(batch, execution.NewRecord(
			rs.aliasedFields,
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
			return errors.Wrapf(err, "couldn't push csv record with index %d in batch to output record queue", i)
		}

		log.Println("csv worker: record pushed: ", batch[i])
	}

	if err := rs.saveOffset(tx, len(batch)); err != nil {
		return errors.Wrap(err, "couldn't save csv offset")
	}

	return nil
}

func (rs *RecordStream) initializeColumnsWithHeaderRow() error {
	columns, err := rs.r.Read()
	if err != nil {
		return errors.Wrap(err, "couldn't read row")
	}

	rs.aliasedFields = make([]octosql.VariableName, len(columns))
	for i, c := range columns {
		rs.aliasedFields[i] = octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, c))
	}

	rs.r.FieldsPerRecord = len(rs.aliasedFields)

	set := make(map[octosql.VariableName]struct{})
	for _, f := range rs.aliasedFields {
		if _, present := set[f]; present {
			return errors.New("column names not unique")
		}
		set[f] = struct{}{}
	}

	return nil
}

func (rs *RecordStream) initializeColumnsWithoutHeaderRow() (*execution.Record, error) {
	row, err := rs.r.Read()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read row")
	}

	rs.aliasedFields = make([]octosql.VariableName, len(row))
	for i := range row {
		rs.aliasedFields[i] = octosql.NewVariableName(fmt.Sprintf("%s.col%d", rs.alias, i+1))
	}

	return execution.NewRecordFromSlice(rs.aliasedFields, parseDataTypes(row),
		execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(rs.streamID, 0))), nil
}

func parseDataTypes(row []string) []octosql.Value {
	resultRow := make([]octosql.Value, len(row))
	for i, v := range row {
		resultRow[i] = execution.ParseType(v)
	}

	return resultRow
}

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

func (rs *RecordStream) saveOffset(tx storage.StateTransaction, curBatchSize int) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	rs.offset = rs.offset + curBatchSize

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
	case *QueueElement_Error:
		if queueElement.Error == execution.ErrEndOfStream.Error() {
			return nil, execution.ErrEndOfStream
		}

		return nil, errors.New(queueElement.Error)
	default:
		panic("invalid queue element type")
	}
}

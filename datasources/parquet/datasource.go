package parquet

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/kostya-sh/parquet-go/parquet"
	"github.com/kostya-sh/parquet-go/parquetformat"
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

type ColStrIter struct {
	file *parquet.File
	col  parquet.Column

	// Only one of these is used at a time
	bools      []bool
	int32s     []int32
	int64s     []int64
	int96s     []parquet.Int96
	float32s   []float32
	float64s   []float64
	byteArrays [][]byte

	values interface{}

	dLevels []uint16 // definition levels
	rLevels []uint16 // repetition levels

	columnChunkReader      *parquet.ColumnChunkReader
	rowGroups              int
	numberOfLoadedElements int
	descriptionIterator    int
	dataIterator           int
}

func NewColStrIter(f *parquet.File, col parquet.Column) *ColStrIter {
	const batchSize = 1024

	it := ColStrIter{
		file:    f,
		col:     col,
		dLevels: make([]uint16, batchSize),
		rLevels: make([]uint16, batchSize),
	}

	switch col.Type() {
	case parquetformat.Type_BOOLEAN:
		it.bools = make([]bool, batchSize)
		it.values = it.bools
	case parquetformat.Type_INT32:
		it.int32s = make([]int32, batchSize)
		it.values = it.int32s
	case parquetformat.Type_INT64:
		it.int64s = make([]int64, batchSize)
		it.values = it.int64s
	case parquetformat.Type_INT96:
		it.int96s = make([]parquet.Int96, batchSize)
		it.values = it.int96s
	case parquetformat.Type_FLOAT:
		it.float32s = make([]float32, batchSize)
		it.values = it.float32s
	case parquetformat.Type_DOUBLE:
		it.float64s = make([]float64, batchSize)
		it.values = it.float64s
	case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
		it.byteArrays = make([][]byte, batchSize)
		it.values = it.byteArrays
	default:
		panic("unknown type")
	}

	return &it
}

var ErrNoNewElement = errors.New("all groups have been processed and there is no new element")

func (it *ColStrIter) Next() (interface{}, error) {
	var err error
	var value interface{}
	elements := make([]interface{}, 0)

	for {
		// If there is no active row group create one
		if it.columnChunkReader == nil {

			// If all row groups have been processed return current value
			if it.rowGroups == len(it.file.MetaData.RowGroups) {
				if len(elements) > 0 {
					return elements, nil
				}
				return nil, ErrNoNewElement
			}

			it.columnChunkReader, err = it.file.NewReader(it.col, it.rowGroups)
			if err != nil {
				return nil, err
			}

			it.rowGroups++
		}

		// If all data in chunk has been processed load next chunk
		if it.descriptionIterator >= it.numberOfLoadedElements {
			it.numberOfLoadedElements, err = it.columnChunkReader.Read(it.values, it.dLevels, it.rLevels)
			if err == parquet.EndOfChunk {
				it.columnChunkReader = nil
				continue
			} else if err != nil {
				return nil, err
			}

			it.descriptionIterator = 0
			it.dataIterator = 0
		}

		// If new record is reached return current value
		if it.rLevels[it.descriptionIterator] == 0 && len(elements) > 0 {
			break
		}

		// If data is not defined insert nil in it's place
		if it.dLevels[it.descriptionIterator] == it.col.MaxD() {
			if it.col.MaxR() == 0 {
				switch it.col.Type() {
				case parquetformat.Type_BOOLEAN:
					value = it.bools[it.dataIterator]
				case parquetformat.Type_INT32:
					value = it.int32s[it.dataIterator]
				case parquetformat.Type_INT64:
					value = it.int64s[it.dataIterator]
				case parquetformat.Type_INT96:
					value = it.int96s[it.dataIterator]
				case parquetformat.Type_FLOAT:
					value = it.float32s[it.dataIterator]
				case parquetformat.Type_DOUBLE:
					value = it.float64s[it.dataIterator]
				case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
					value = it.byteArrays[it.dataIterator]
				default:
					panic("unknown type")
				}
			} else {
				switch it.col.Type() {
				case parquetformat.Type_BOOLEAN:
					elements = append(elements, it.bools[it.dataIterator])
				case parquetformat.Type_INT32:
					elements = append(elements, it.int32s[it.dataIterator])
				case parquetformat.Type_INT64:
					elements = append(elements, it.int64s[it.dataIterator])
				case parquetformat.Type_INT96:
					elements = append(elements, it.int96s[it.dataIterator])
				case parquetformat.Type_FLOAT:
					elements = append(elements, it.float32s[it.dataIterator])
				case parquetformat.Type_DOUBLE:
					elements = append(elements, it.float64s[it.dataIterator])
				case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
					elements = append(elements, it.byteArrays[it.dataIterator])
				default:
					panic("unknown type")
				}
			}

			it.dataIterator++
		} else {
			elements = append(elements, nil)
		}

		it.descriptionIterator++

		// If data is not repeated stop reading after first element
		if it.col.MaxR() == 0 {
			break
		}
	}

	// If column has repeated elements return list instead of single value
	if it.col.MaxR() > 0 {
		value = elements
	}

	return value, nil
}

type DataSource struct {
	path         string
	alias        string
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
			batchSize, err := config.GetInt(dbConfig, "batchSize", config.WithDefault(1000))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get batch size")
			}

			return &DataSource{
				path:         path,
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
		stateStorage: ds.stateStorage,
		streamID:     streamID,
		filePath:     ds.path,
		isDone:       false,
		alias:        ds.alias,
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
					err := errors.Wrap(err, "parquet worker error")
					log.Println("parquet error: ", err)
					rs.workerCloseErrChan <- err
					return err
				}
			}},
		),
		nil
}

type RecordStream struct {
	stateStorage        storage.Storage
	streamID            *execution.StreamID
	filePath            string
	file                *parquet.File
	columnIterators     []*ColStrIter
	columnNames         []octosql.VariableName
	columns             []parquet.Column
	columnsLogicalTypes []*parquetformat.LogicalType
	isDone              bool
	alias               string
	offset              int
	batchSize           int

	workerCtxCancel    func()
	workerCloseErrChan chan error
}

func (rs *RecordStream) Close(ctx context.Context, storage storage.Storage) error {
	rs.workerCtxCancel()
	err := <-rs.workerCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop parquet worker")
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
			return errors.Wrap(err, "couldn't reinitialize offset for parquet read batch worker")
		}

		tx.Abort() // We only read data above, no need to risk failing now.

		// Load/Reload file
		file, err := parquet.OpenFile(rs.filePath)
		if err != nil {
			return errors.Wrap(err, "couldn't open file")
		}
		rs.file = file

		columns := file.Schema.Columns()
		for _, col := range columns {
			if col.MaxR() > 1 {
				return errors.Errorf("not supported nested repeated elements in column '%s'", col)
			}
		}
		rs.columns = columns

		rs.columnNames = make([]octosql.VariableName, len(columns))
		for i := range columns {
			rs.columnNames[i] = octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, columns[i].String()))
		}

		columnLogicalTypes := make(map[string]*parquetformat.LogicalType)
		for _, schemaElement := range file.MetaData.Schema {
			columnLogicalTypes[schemaElement.Name] = schemaElement.LogicalType
		}

		rs.columnsLogicalTypes = make([]*parquetformat.LogicalType, len(rs.columnNames))
		for i, column := range rs.columns {
			rs.columnsLogicalTypes[i] = columnLogicalTypes[column.String()]
		}

		colIters := make([]*ColStrIter, len(columns))
		for i, col := range columns {
			colIters[i] = NewColStrIter(file, col)
		}
		rs.columnIterators = colIters

		// Moving file iterator by `rs.offset`
		for i := 0; i < rs.offset; i++ {
			_, err := rs.readRecordFromFile()
			if err == execution.ErrEndOfStream {
				return ctx.Err()
			} else if err != nil {
				return errors.Wrapf(err, "couldn't move parquet file iterator by %d offset", rs.offset)
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
					log.Println("parquet worker: couldn't listen for changes: ", err)
				}
				err = waitableError.Close()
				if err != nil {
					log.Println("parquet worker: couldn't close storage changes subscription: ", err)
				}
				continue
			} else if err == execution.ErrEndOfStream {
				err = tx.Commit()
				if err != nil {
					log.Println("parquet worker: couldn't commit transaction: ", err)
					continue
				}
				return ctx.Err()
			} else if err != nil {
				tx.Abort()
				log.Printf("parquet worker: error running parquet read batch worker: %s, reinitializing from storage", err)
				break
			}

			err = tx.Commit()
			if err != nil {
				log.Println("parquet worker: couldn't commit transaction: ", err)
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
			return errors.Wrapf(err, "couldn't push parquet EndOfStream to output record queue")
		}

		return execution.ErrEndOfStream
	}

	batch := make([]*execution.Record, 0)
	for i := 0; i < rs.batchSize; i++ {
		recordValues, err := rs.readRecordFromFile()
		if err == execution.ErrEndOfStream {
			break
		} else if err != nil {
			return errors.Wrap(err, "couldn't read record from parquet file")
		}

		octoValues := make([]octosql.Value, len(rs.columns))
		for i, v := range recordValues {
			if int96, ok := v.(parquet.Int96); ok {
				v = int(binary.LittleEndian.Uint64(int96[:8]))
			}
			// Info here: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
			if logicalType := rs.columnsLogicalTypes[i]; logicalType != nil && v != nil {
				var convert func(interface{}) interface{}

				switch {
				case logicalType.STRING != nil:
					convert = func(v interface{}) interface{} {
						return string(v.([]byte))
					}
				case logicalType.DECIMAL != nil:
					convert = func(v interface{}) interface{} {
						switch v := v.(type) {
						case int32:
							return float64(v) * math.Pow10(int(-logicalType.DECIMAL.Scale))
						case int64:
							return float64(v) * math.Pow10(int(-logicalType.DECIMAL.Scale))
						case []byte:
							// The byte slice represents a big endian two's complement signed integer
							var value float64
							// This reads the unsigned value.
							for i := range v {
								j := len(v) - i - 1
								value += float64(v[j]) * math.Pow(2, float64(8*i))
							}
							// This applies two's complement to get a signed one.
							if v[0]&0b10000000 == 0b10000000 {
								value -= math.Pow(2, float64(8*len(v)))
							}
							return value * math.Pow10(int(-logicalType.DECIMAL.Scale))
						default:
							return v
						}
					}
				case logicalType.DATE != nil:
					convert = func(v interface{}) interface{} {
						epochStart := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
						return epochStart.Add(time.Hour * 24 * time.Duration(v.(int32)))
					}
				case logicalType.TIME != nil:
					convert = func(v interface{}) interface{} {
						// We try to match each supported unit.
						if logicalType.TIME.Unit.IsSetMILLIS() {
							return time.Millisecond * time.Duration(v.(int32))
						} else if logicalType.TIME.Unit.IsSetMICROS() {
							return time.Microsecond * time.Duration(v.(int64))
						}
						return v
					}
				case logicalType.TIMESTAMP != nil:
					convert = func(v interface{}) interface{} {
						value := v.(int64)
						if logicalType.TIMESTAMP.IsAdjustedToUTC {
							var seconds int64
							var nanoseconds int64
							// We try to match each supported unit.
							if logicalType.TIMESTAMP.Unit.IsSetMILLIS() {
								millisecondsInSecond := int64(time.Second / time.Millisecond)
								nanosecondsInMillisecond := int64(time.Millisecond / time.Nanosecond)
								seconds = value / millisecondsInSecond
								nanoseconds = (value % millisecondsInSecond) * nanosecondsInMillisecond
								return time.Unix(seconds, nanoseconds)
							} else if logicalType.TIMESTAMP.Unit.IsSetMICROS() {
								microsecondsInSecond := int64(time.Second / time.Microsecond)
								nanosecondsInMicrosecond := int64(time.Microsecond / time.Nanosecond)
								seconds = value / microsecondsInSecond
								nanoseconds = (value % microsecondsInSecond) * nanosecondsInMicrosecond
								return time.Unix(seconds, nanoseconds)
							}
						} else {
							epochStart := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
							// We try to match each supported unit.
							if logicalType.TIMESTAMP.Unit.IsSetMILLIS() {
								return epochStart.Add(time.Millisecond * time.Duration(value))
							} else if logicalType.TIMESTAMP.Unit.IsSetMICROS() {
								return epochStart.Add(time.Microsecond * time.Duration(value))
							}
						}
						return v
					}
				case logicalType.INTEGER != nil:
					convert = func(v interface{}) interface{} {
						if logicalType.INTEGER.IsSigned {
							if logicalType.INTEGER.BitWidth == 64 {
								return v.(int64)
							} else if logicalType.INTEGER.BitWidth == 32 {
								return v.(int32)
							} else if logicalType.INTEGER.BitWidth == 16 {
								return int16(v.(int32))
							} else if logicalType.INTEGER.BitWidth == 8 {
								return int8(v.(int32))
							}
						} else {
							if logicalType.INTEGER.BitWidth == 64 {
								return uint64(v.(int64))
							} else if logicalType.INTEGER.BitWidth == 32 {
								return uint32(v.(int32))
							} else if logicalType.INTEGER.BitWidth == 16 {
								return uint16(v.(int32))
							} else if logicalType.INTEGER.BitWidth == 8 {
								return uint8(v.(int32))
							}
						}
						return v
					}
				case logicalType.JSON != nil:
					convert = func(v interface{}) interface{} {
						var object map[string]interface{}
						if err := json.Unmarshal(v.([]byte), &object); err != nil {
							log.Printf("couldn't decode '%s' as json in parquet: %s", v.([]byte), err.Error())
							return v
						}
						return object
					}
				case logicalType.BSON != nil:
				case logicalType.UUID != nil:
					convert = func(v interface{}) interface{} {
						data := v.([]byte)
						// This is how we readably format uuid bytes
						return fmt.Sprintf("%.8x-%.4x-%.4x-%.4x-%.12x", data[:4], data[4:6], data[6:8], data[8:10], data[10:16])
					}
				}

				if convert != nil {
					switch typed := v.(type) {
					case []interface{}:
						for i := range typed {
							typed[i] = convert(typed[i])
							v = typed
						}
					case interface{}:
						v = convert(v)
					}
				}
			}
			octoValues[i] = octosql.NormalizeType(v)
		}

		batch = append(batch, execution.NewRecordFromSlice(
			rs.columnNames,
			octoValues,
			execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(rs.streamID, rs.offset+i))))
	}

	for i := range batch {
		err := outputQueue.Push(ctx, &QueueElement{
			Type: &QueueElement_Record{
				Record: batch[i],
			},
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't push parquet record with index %d in batch to output record queue", i)
		}
	}

	rs.offset = rs.offset + len(batch)
	if err := rs.saveOffset(tx); err != nil {
		return errors.Wrap(err, "couldn't save parquet offset")
	}

	return nil
}

func (rs *RecordStream) readRecordFromFile() ([]interface{}, error) {
	values := make([]interface{}, len(rs.columns))
	gotNonNilValue := false // Indicates whether we got at least 1 non-nil record field from column iterators

	// If at least one row has remaining elements create new record
	for i, it := range rs.columnIterators {
		value, err := it.Next()
		if err == ErrNoNewElement {
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't decode parquet record")
		} else {
			gotNonNilValue = true
		}

		values[i] = value
	}

	if !gotNonNilValue {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	return values, nil
}

var offsetPrefix = []byte("parquet_offset")

func (rs *RecordStream) loadOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	var offset octosql.Value
	err := offsetState.Get(&offset)
	if err == storage.ErrNotFound {
		offset = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't load parquet offset from state storage")
	}

	rs.offset = offset.AsInt()

	return nil
}

func (rs *RecordStream) saveOffset(tx storage.StateTransaction) error {
	offsetState := storage.NewValueState(tx.WithPrefix(offsetPrefix))

	offset := octosql.MakeInt(rs.offset)
	err := offsetState.Set(&offset)
	if err != nil {
		return errors.Wrap(err, "couldn't save parquet offset to state storage")
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

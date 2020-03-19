package parquet

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/kostya-sh/parquet-go/parquet"
	"github.com/kostya-sh/parquet-go/parquetformat"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path  string
	alias string
}

type ColStrIter struct {
	f   *parquet.File
	col parquet.Column

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

	cr                  *parquet.ColumnChunkReader
	rg                  int // row groups
	n                   int // number of loaded elements
	descriptionIterator int
	dataIterator        int
}

func NewColStrIter(f *parquet.File, col parquet.Column) *ColStrIter {
	const batchSize = 1024

	it := ColStrIter{
		f:       f,
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


func (it *ColStrIter) Next() (interface{}, error) {
	var err error
	var s interface{}
	s = nil
	elements := make([]interface{}, 0)
	for {
		if it.cr == nil {
			if it.rg == len(it.f.MetaData.RowGroups) {
				if len(elements) > 0 {
					return elements, nil
				}
				return nil, io.EOF
			}
			it.cr, err = it.f.NewReader(it.col, it.rg)
			if err != nil {
				return nil, err
			}
			it.rg++
		}

		if it.descriptionIterator >= it.n {
			it.n, err = it.cr.Read(it.values, it.dLevels, it.rLevels)
			if err == parquet.EndOfChunk {
				it.cr = nil
				return it.Next()
			}
			if err != nil {
				return nil, err
			}
			it.descriptionIterator = 0
			it.dataIterator = 0
		}

		if it.rLevels[it.descriptionIterator] == 0 && len(elements) > 0 {
			break;
		}

		if it.dLevels[it.descriptionIterator] == it.col.MaxD() {
			if it.col.MaxR() == 0 {
				switch it.col.Type() {
				case parquetformat.Type_BOOLEAN:
					s = it.bools[it.dataIterator]
				case parquetformat.Type_INT32:
					s = it.int32s[it.dataIterator]
				case parquetformat.Type_INT64:
					s = it.int64s[it.dataIterator]
				case parquetformat.Type_INT96:
					s = it.int96s[it.dataIterator]
				case parquetformat.Type_FLOAT:
					s = it.float32s[it.dataIterator]
				case parquetformat.Type_DOUBLE:
					s = it.float64s[it.dataIterator]
				case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
					s = it.byteArrays[it.dataIterator]
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

		if it.col.MaxR() == 0 {
			break;
		}
	}

	if it.col.MaxR() > 0 {
		s = elements
	}
	return s, nil
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}

			return &DataSource{
				path:  path,
				alias: alias,
			}, nil
		},
		nil,
		availableFilters,
		metadata.BoundedFitsInLocalStorage,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewDataSourceBuilderFactory(), nil
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables) (execution.RecordStream, error) {
	file, err := parquet.OpenFile(ds.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}
	columns := file.Schema.Columns()
	n := len(columns)
	for _, col := range columns {
		if col.MaxR() > 1 {
			return nil, fmt.Errorf("column '%s' has nested repeated elements", col)
		}
	}

	colIters := make([]*ColStrIter, n)
	for i, col := range columns {
		colIters[i] = NewColStrIter(file, col)
	}

	return &RecordStream{
		file:            file,
		isDone:          false,
		columnIterators: colIters,
		columns:         columns,
		alias:           ds.alias,
	}, nil
}

type RecordStream struct {
	file            *parquet.File
	columnIterators []*ColStrIter
	columns         []parquet.Column
	isDone          bool
	alias           string
}

func (rs *RecordStream) Close() error {
	err := rs.file.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close underlying file")
	}

	return nil
}

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
	if rs.isDone {
		err := rs.file.Close()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't close underlying file")
		}
		return nil, execution.ErrEndOfStream
	}

	m := make(map[string]interface{})

	rs.isDone = true

	for i, it := range rs.columnIterators {
		s, err := it.Next()
		if err != nil {
			if err == io.EOF {
				s = nil
			} else {
				return nil, errors.Wrap(err, "couldn't decode parquet record")
			}
		} else {
			rs.isDone = false
		}
		m[rs.columns[i].String()] = s
	}

	if rs.isDone == true {
		err := rs.file.Close()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't close underlying file")
		}
		return nil, execution.ErrEndOfStream
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for k, v := range m {
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

	return execution.NewRecord(fields, aliasedRecord), nil
}

package csv

// only comma-separated now. Changing that means setting RecordStream.r.Coma
// .csv reader trims leading white space(s)

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

// for csv/datasource_test.go usage only
func newDataSource(path, alias string) *DataSource {
	return &DataSource{
		path:  path,
		alias: alias,
	}
}

type DataSource struct {
	path  string
	alias string
}

func NewDataSourceBuilderFactory(path string) func(alias string) *physical.DataSourceBuilder {
	return physical.NewDataSourceBuilderFactory(
		func(filter physical.Formula, alias string) (execution.Node, error) {
			return &DataSource{
				path:  path,
				alias: alias,
			}, nil
		},
		nil,
		availableFilters,
	)
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	file, err := os.Open(ds.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}
	r := csv.NewReader(bufio.NewReader(file))
	r.TrimLeadingSpace = true

	columns, err := r.Read()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read column names")
	}

	aliasedFields := make([]octosql.VariableName, 0)
	for _, c := range columns {
		aliasedFields = append(aliasedFields, octosql.VariableName(fmt.Sprintf("%s.%s", ds.alias, c)))
	}
	r.FieldsPerRecord = len(aliasedFields)

	set := make(map[octosql.VariableName]struct{})
	for _, f := range aliasedFields {
		if _, present := set[f]; present {
			return nil, errors.New("column names not unique") // cannot use Wrap() :(
		}
		set[f] = struct{}{}
	}

	return &RecordStream{
		file:          file,
		r:             r,
		isDone:        false,
		alias:         ds.alias,
		aliasedFields: aliasedFields,
	}, nil
}

type RecordStream struct {
	file          *os.File
	r             *csv.Reader
	isDone        bool
	alias         string
	aliasedFields []octosql.VariableName
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	line, err := rs.r.Read()
	if err == io.EOF {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}

	if err != nil {
		return nil, errors.Wrap(err, "couldn't read record")
	}

	aliasedRecord := make(map[octosql.VariableName]interface{})
	for i, v := range line {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(rs.aliasedFields, aliasedRecord), nil
}

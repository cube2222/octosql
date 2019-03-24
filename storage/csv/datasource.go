package csv

//only comma-separated now. Changing that means setting RecordStream.rdr.Coma

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

// at the moment no "fields" field there - I cannot see how it could be used; easy to implement, though
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
	rdr := csv.NewReader(bufio.NewReader(file))

	columns, err := rdr.Read()
	if err != nil {
		return nil, errors.Wrap(err, "problem parsing column names")
	}

	fields := make([]octosql.VariableName, 0)
	for _, c := range columns {
		fields = append(fields, octosql.VariableName(c))
	}
	rdr.FieldsPerRecord = len(fields)

	var set map[octosql.VariableName]interface{}
	for _, f := range fields {
		if _, present := set[f]; present {
			return nil, errors.New("column names not unique") //cannot use Wrap() :(
		}
		set[f] = nil // is it idiomatic?
	}

	return &RecordStream{
		file:   file,
		rdr:    rdr,
		isDone: false,
		alias:  ds.alias,
		fields: fields,
	}, nil
}

type RecordStream struct {
	file   *os.File
	rdr    *csv.Reader
	isDone bool
	alias  string
	fields []octosql.VariableName
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	line, err := rs.rdr.Read()
	if err == io.EOF {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}

	if err != nil {
		return nil, errors.Wrap(err, "problem(s) reading record")
	}

	var record map[octosql.VariableName]interface{}
	for i, v := range line {
		record[rs.fields[i]] = execution.NormalizeType(execution.ParseType(v)) //is ParseType needed?
	}

	aliasedRecord := make(map[octosql.VariableName]interface{})
	for k, v := range record {
		aliasedRecord[octosql.VariableName(fmt.Sprintf("%s.%s", rs.alias, k))] = v
	}

	return execution.NewRecord(rs.fields, aliasedRecord), nil
}

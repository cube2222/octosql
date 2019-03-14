package json

import (
	"bufio"
	"encoding/json"
	"fmt"
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
	sc := bufio.NewScanner(file)
	return &RecordStream{
		file:   file,
		sc:     sc,
		isDone: false,
		alias:  ds.alias,
	}, nil
}

type RecordStream struct {
	file   *os.File
	sc     *bufio.Scanner
	isDone bool
	alias  string
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.sc.Scan() {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}

	var record map[octosql.VariableName]interface{}
	err := json.Unmarshal(rs.sc.Bytes(), &record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't unmarshal json record")
	}

	aliasedRecord := make(map[octosql.VariableName]interface{})
	for k, v := range record {
		aliasedRecord[octosql.VariableName(fmt.Sprintf("%s.%s", rs.alias, k))] = v
	}

	fields := make([]octosql.VariableName, 0)
	for k := range aliasedRecord {
		fields = append(fields, k)
	}

	return execution.NewRecord(fields, aliasedRecord), nil
}

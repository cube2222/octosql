package csv

// only comma-separated now. Changing that means setting RecordStream.r.Coma
// .csv reader trims leading white space(s)

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

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
	path           string
	alias          string
	hasColumnNames bool
	separator      rune
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

			return &DataSource{
				path:           path,
				alias:          alias,
				hasColumnNames: hasColumns,
				separator:      r,
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
	file, err := os.Open(ds.path)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't open file")
	}
	r := csv.NewReader(file)
	r.Comma = ds.separator
	r.TrimLeadingSpace = true

	return &RecordStream{
		streamID:        streamID,
		file:            file,
		r:               r,
		isDone:          false,
		alias:           ds.alias,
		first:           true,
		hasColumnHeader: ds.hasColumnNames,
	}, execution.NewExecutionOutput(execution.NewZeroWatermarkGenerator()), nil
}

type RecordStream struct {
	streamID        *execution.StreamID
	file            *os.File
	r               *csv.Reader
	isDone          bool
	alias           string
	aliasedFields   []octosql.VariableName
	first           bool
	hasColumnHeader bool
	offset          int
}

func (rs *RecordStream) Close() error {
	err := rs.file.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close underlying file")
	}

	return nil
}

func parseDataTypes(row []string) []octosql.Value {
	resultRow := make([]octosql.Value, len(row))
	for i, v := range row {
		resultRow[i] = execution.ParseType(v)
	}

	return resultRow
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

	return execution.NewRecordFromSlice(rs.aliasedFields, parseDataTypes(row)), nil
}

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if rs.first {
		rs.first = false

		if rs.hasColumnHeader {
			err := rs.initializeColumnsWithHeaderRow()
			if err != nil {
				return nil, errors.Wrap(err, "couldn't initialize columns for record stream")
			}

			return rs.Next(ctx)
		} else {
			record, err := rs.initializeColumnsWithoutHeaderRow()
			if err != nil {
				return nil, errors.Wrap(err, "couldn't initialize columns for record stream")
			}

			return record, nil
		}
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

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range line {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	curOffset := rs.offset
	rs.offset++
	return execution.NewRecord(
		rs.aliasedFields,
		aliasedRecord,
		execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(rs.streamID, curOffset)),
	), nil
}

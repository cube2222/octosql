package excel

import (
	"context"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path           string
	alias          string
	hasColumnNames bool
	sheet          string
	root           string
}

var ErrEmptyRow error = errors.New("the row is empty")

func readRow(file *excelize.File, sheet string, rootCell cell) ([]string, error) {
	result := make([]string, 0)

	for true {
		value := file.GetCellValue(sheet, rootCell.getCellName())
		if value == "" {
			break
		}

		result = append(result, value)
		rootCell = rootCell.getCellToTheRight()
	}

	if len(result) == 0 {
		return nil, ErrEmptyRow
	}

	return result, nil
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}

			hasColumns, err := config.GetBool(dbConfig, "columns", config.WithDefault(true))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get column option")
			}

			sheet, err := config.GetString(dbConfig, "sheet", config.WithDefault("Sheet1"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get sheet name")
			}

			root, err := config.GetString(dbConfig, "root", config.WithDefault("A1"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get root cell")
			}

			return &DataSource{
				path:           path,
				alias:          alias,
				hasColumnNames: hasColumns,
				sheet:          sheet,
				root:           root,
			}, nil
		},
		nil,
		availableFilters,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewDataSourceBuilderFactory(), nil
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	file, err := excelize.OpenFile(ds.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}

	rootCell, err := getCellFromName(ds.root)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse root cell")
	}

	var columns []string

	/* if the files has column names then it's the first row */
	if ds.hasColumnNames {
		columns, err = readRow(file, ds.sheet, rootCell)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't read column names")
		}

	} else { /* otherwise we must determine how many rows there are and create columns col1, col2... */
		tempColumns, err := readRow(file, ds.sheet, rootCell)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get first row to determine number of rows")
		}

		columnCount := len(tempColumns)

		columns = make([]string, columnCount)

		for i := 0; i < columnCount; i++ {
			columns[i] = fmt.Sprintf("col%d", i+1)
		}
	}

	aliasedFields := make([]octosql.VariableName, 0)
	for _, c := range columns {
		aliasedFields = append(aliasedFields, octosql.VariableName(fmt.Sprintf("%s.%s", ds.alias, c)))
	}

	set := make(map[octosql.VariableName]struct{})
	for _, f := range aliasedFields {
		if _, present := set[f]; present {
			return nil, errors.New("column names not unique")
		}
		set[f] = struct{}{}
	}

	if ds.hasColumnNames {
		rootCell = rootCell.getCellBelow()
	}

	return &RecordStream{
		file:                file,
		sheet:               ds.sheet,
		isDone:              false,
		alias:               ds.alias,
		aliasedFields:       aliasedFields,
		currentRowStartCell: rootCell,
	}, nil
}

type RecordStream struct {
	file                *excelize.File
	sheet               string
	isDone              bool
	alias               string
	aliasedFields       []octosql.VariableName
	currentRowStartCell cell
}

func (rs *RecordStream) Close() error {
	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	line, err := readRow(rs.file, rs.sheet, rs.currentRowStartCell)
	if err == ErrEmptyRow {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	if err != nil {
		return nil, errors.Wrap(err, "couldn't read record")
	}

	rs.currentRowStartCell = rs.currentRowStartCell.getCellBelow()

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range line {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(rs.aliasedFields, aliasedRecord), nil
}

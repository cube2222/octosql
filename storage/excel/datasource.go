package excel

import (
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path             string
	alias            string
	hasColumnNames   bool
	sheet            string
	rootColumnString string
	rootColumn       int
	rootRow          int
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}

			hasColumns, err := config.GetBool(dbConfig, "headerRow", config.WithDefault(true))
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

			if !isCellNameValid(root) {
				return nil, errors.Wrap(err, "the root cell of the table is invalid")
			}

			_, colStr := getRowAndColumnFromCell(root)

			row, col, err := getCoordinatesFromCell(root)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't extract column and row numbers from cell")
			}

			return &DataSource{
				path:             path,
				alias:            alias,
				hasColumnNames:   hasColumns,
				sheet:            sheet,
				rootColumn:       col,
				rootRow:          row,
				rootColumnString: colStr,
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

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	file, err := excelize.OpenFile(ds.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}

	rows, err := file.Rows(ds.sheet)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get sheet's rows")
	}

	for i := 0; i <= ds.rootRow; i++ { /* skip some potential empty rows */
		if !rows.Next() {
			return nil, errors.New("encountered an error while omitting initial rows")
		}
	}

	return &RecordStream{
		isDone:           false,
		first:            true,
		hasHeaderRow:     ds.hasColumnNames,
		alias:            ds.alias,
		columnOffset:     ds.rootColumn,
		rows:             rows,
		currentRowNumber: ds.rootRow + 1,
		file:             file,
		rootColumnName:   ds.rootColumnString,
		sheet:            ds.sheet,
	}, nil
}

type RecordStream struct {
	isDone        bool
	alias         string
	aliasedFields []octosql.VariableName
	rows          *excelize.Rows
	hasHeaderRow  bool
	first         bool
	columnOffset  int
	file          *excelize.File

	sheet            string
	currentRowNumber int
	rootColumnName   string
}

func (rs *RecordStream) Close() error {
	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if rs.first {
		rs.first = false

		if rs.hasHeaderRow {
			err := rs.initializeColumnsWithHeaderRow()
			if err != nil {
				return nil, errors.Wrap(err, "couldn't initialize columns for record stream from first row")
			}

			return rs.Next()
		} else {
			rec, err := rs.initializeColumnsWithoutHeaderRow()
			if err != nil {
				return nil, errors.Wrap(err, "couldn't initialize columns")
			}

			return rec, nil
		}
	}

	if !rs.rows.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	row, err := rs.extractRow(rs.rows.Columns())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract row")
	}

	if isRowEmpty(row) {
		return nil, execution.ErrEndOfStream
	}

	for i, v := range row {
		if v == octosql.ZeroString() {
			row[i] = octosql.MakeNull()
		}
	}

	return execution.NewRecordFromSlice(rs.aliasedFields, row), nil
}

package excel

import (
	"context"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/tvf"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path             string
	alias            string
	hasHeaderRow     bool
	sheet            string
	timeColumns      []string
	horizontalOffset int
	verticalOffset   int
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}

			hasHeaderRow, err := config.GetBool(dbConfig, "headerRow", config.WithDefault(true))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get header row option")
			}

			sheet, err := config.GetString(dbConfig, "sheet", config.WithDefault("Sheet1"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get sheet name")
			}

			rootCell, err := config.GetString(dbConfig, "rootCell", config.WithDefault("A1"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get root cell")
			}

			timeColumns, err := config.GetStringList(dbConfig, "timeColumns", config.WithDefault([]string{}))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get time columns")
			}

			verticalOffset, horizontalOffset, err := getCoordinatesFromCell(rootCell)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't extract column and row numbers from root cell")
			}

			return &DataSource{
				path:             path,
				alias:            alias,
				hasHeaderRow:     hasHeaderRow,
				sheet:            sheet,
				timeColumns:      timeColumns,
				horizontalOffset: horizontalOffset,
				verticalOffset:   verticalOffset,
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

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecOutput, error) {
	file, err := excelize.OpenFile(ds.path)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't open file")
	}

	rows, err := file.Rows(ds.sheet)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get sheet's rows")
	}

	for i := 0; i <= ds.verticalOffset; i++ {
		if !rows.Next() {
			return nil, nil, errors.New("root cell is lower than row count")
		}
	}

	return &RecordStream{
		first:            true,
		hasHeaderRow:     ds.hasHeaderRow,
		timeColumnNames:  ds.timeColumns,
		alias:            ds.alias,
		horizontalOffset: ds.horizontalOffset,

		isDone: false,
		rows:   rows,
	}, execution.NewExecOutput(tvf.NewZeroWatermarkGenerator()), nil
}

func contains(xs []string, x string) bool {
	for i := range xs {
		if x == xs[i] {
			return true
		}
	}
	return false
}

type RecordStream struct {
	first            bool
	hasHeaderRow     bool
	timeColumnNames  []string
	alias            string
	horizontalOffset int

	isDone      bool
	columnNames []octosql.VariableName
	timeColumns []bool
	rows        *excelize.Rows
}

func (rs *RecordStream) Close() error {
	return nil
}

func (rs *RecordStream) Next(context.Context) (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	curRow := rs.rows.Columns()

	if rs.first {
		rs.first = false

		var cols []octosql.VariableName
		var err error
		if rs.hasHeaderRow {
			cols, err = getHeaderRow(rs.alias, rs.horizontalOffset, curRow)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get header row")
			}

			if !rs.rows.Next() {
				rs.isDone = true
				return nil, execution.ErrEndOfStream
			}
			curRow = rs.rows.Columns()
		} else {
			cols, err = generatePlaceholders(rs.alias, rs.horizontalOffset, curRow)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't generate placeholder headers")
			}
		}

		rs.columnNames = cols

		rs.timeColumns = make([]bool, len(cols))
		for i := range cols {
			if contains(rs.timeColumnNames, cols[i].Name()) {
				rs.timeColumns[i] = true
			}
		}
	}

	row, err := rs.extractRow(curRow)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract row")
	}

	if isRowEmpty(row) {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	out := execution.NewRecordFromSlice(rs.columnNames, row)

	if !rs.rows.Next() {
		rs.isDone = true
	}

	return out, nil
}

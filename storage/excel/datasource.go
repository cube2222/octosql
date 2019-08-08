package excel

import (
	"fmt"
	"regexp"
	"strconv"
	"unicode"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
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
	rootColumn     int
	rootRow        int
}

func isCellNameValid(cell string) bool {
	cellNameRegexp := "^[A-Z]+[1-9][0-9]*$"

	r, _ := regexp.Compile(cellNameRegexp)

	return r.MatchString(cell)
}

func getCellRowCol(cell string) (row, col string) {
	for index, char := range cell {
		if unicode.IsDigit(char) {
			return cell[index:], cell[:index]
		}
	}

	return "", cell
}

func getRowColCoords(cell string) (row int, col int, err error) {
	rowStr, colStr := getCellRowCol(cell)
	col = excelize.TitleToNumber(colStr)
	rowTmp, err := strconv.ParseInt(rowStr, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrap(err, "couldn't parse row")
	}

	return int(rowTmp) - 1, col, nil
}

func normalizeRows(rows [][]string, col int, row int) [][]string {
	result := make([][]string, 0)

	for i := row; i < len(rows); i++ {
		result = append(result, rows[i][col:])
	}

	return result
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}

			//TODO: don't hardcode fields in config parsing
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

			row, col, err := getRowColCoords(root)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't extract column and row numbers from cell")
			}

			return &DataSource{
				path:           path,
				alias:          alias,
				hasColumnNames: hasColumns,
				sheet:          sheet,
				rootColumn:     col,
				rootRow:        row,
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

	rows := file.GetRows(ds.sheet)
	if len(rows) <= ds.rootColumn {
		return nil, errors.New("the sheet doesn't have enough columns starting from root cell")
	}

	rows = normalizeRows(rows, ds.rootColumn, ds.rootRow)

	var columns []string

	/* if the files has column names then it's the first row */
	if ds.hasColumnNames {
		columns = rows[0]

	} else { /* otherwise we must determine how many rows there are and create columns col1, col2... */
		columnCount := len(rows[0])

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

	startingRow := 0
	if ds.hasColumnNames {
		startingRow = 1
	}

	return &RecordStream{
		isDone:          false,
		alias:           ds.alias,
		aliasedFields:   aliasedFields,
		rows:            rows,
		currentRowIndex: startingRow,
	}, nil
}

type RecordStream struct {
	isDone          bool
	alias           string
	aliasedFields   []octosql.VariableName
	rows            [][]string
	currentRowIndex int
}

func (rs *RecordStream) Close() error {
	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if rs.currentRowIndex >= len(rs.rows) {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range rs.rows[rs.currentRowIndex] {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	rs.currentRowIndex++

	return execution.NewRecord(rs.aliasedFields, aliasedRecord), nil
}

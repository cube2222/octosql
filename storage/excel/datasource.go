package excel

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

	rows, err := file.Rows(ds.sheet)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get sheet's rows")
	}

	for i := 0; i <= ds.rootRow; i++ { /* skip some potential empty rows */
		rows.Next()
	}

	var columns []string
	var firstRow []string

	/* if the files has column names then it's the first row */
	if ds.hasColumnNames {
		columns = rows.Columns()[ds.rootColumn:]

	} else {
		/*
			otherwise we must determine how many rows there are and create columns col1, col2...
			but we need to read the first row, which we will have to store to later return
			during the first call of Next()
		*/
		firstRow = rows.Columns()[ds.rootColumn:]

		columnCount := len(firstRow)
		columns = make([]string, columnCount)

		for i := 0; i < columnCount; i++ {
			columns[i] = fmt.Sprintf("col%d", i+1)
		}
	}

	aliasedFields := make([]octosql.VariableName, 0)
	for _, c := range columns {
		lowerCased := strings.ToLower(fmt.Sprintf("%s.%s", ds.alias, c))
		aliasedFields = append(aliasedFields, octosql.VariableName(lowerCased))
	}

	set := make(map[octosql.VariableName]struct{})
	for _, f := range aliasedFields {
		if _, present := set[f]; present {
			return nil, errors.New("column names not unique")
		}
		set[f] = struct{}{}
	}

	return &RecordStream{
		isDone:            false,
		alias:             ds.alias,
		aliasedFields:     aliasedFields,
		rows:              rows,
		potentialFirstRow: firstRow,
		hasFirstRowLoaded: !ds.hasColumnNames,
		columnOffset:      ds.rootColumn,
	}, nil
}

type RecordStream struct {
	isDone            bool
	alias             string
	aliasedFields     []octosql.VariableName
	rows              *excelize.Rows
	potentialFirstRow []string
	hasFirstRowLoaded bool
	columnOffset      int
}

func (rs *RecordStream) Close() error {
	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	var row []string

	if rs.hasFirstRowLoaded {
		rs.hasFirstRowLoaded = false
		row = rs.potentialFirstRow
	} else {
		if !rs.rows.Next() {
			rs.isDone = true
			rs.Close()
			return nil, execution.ErrEndOfStream
		}

		row = rs.rows.Columns()[rs.columnOffset:]
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range row {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(rs.aliasedFields, aliasedRecord), nil
}

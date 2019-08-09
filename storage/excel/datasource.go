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
	"github.com/cube2222/octosql/physical/metadata"
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
		metadata.BoundedDoesntFitInLocalStorage,
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
		isDone:       false,
		alias:        ds.alias,
		rows:         rows,
		hasHeaderRow: ds.hasColumnNames,
		first:        true,
		columnOffset: ds.rootColumn,
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
}

func (rs *RecordStream) Close() error {
	return nil
}

func extractRow(row []string, columnOffset int) []string {
	trimmedRow := make([]string, 0)

	for i := columnOffset; i < len(row); i++ {
		if row[i] != "" {
			trimmedRow = append(trimmedRow, row[i])
		} else {
			break
		}
	}

	return trimmedRow
}

func (rs *RecordStream) initializeColumnsWithHeaderRow() error {
	columns := extractRow(rs.rows.Columns(), rs.columnOffset)

	rs.aliasedFields = make([]octosql.VariableName, 0)
	for _, c := range columns {
		lowerCased := strings.ToLower(fmt.Sprintf("%s.%s", rs.alias, c))
		rs.aliasedFields = append(rs.aliasedFields, octosql.VariableName(lowerCased))
	}

	set := make(map[octosql.VariableName]struct{})
	for _, f := range rs.aliasedFields {
		if _, present := set[f]; present {
			return errors.New("column names not unique")
		}
		set[f] = struct{}{}
	}

	return nil
}

func (rs *RecordStream) initializeColumnsWithoutHeaderRow() *execution.Record {
	firstRow := extractRow(rs.rows.Columns(), rs.columnOffset)

	rs.aliasedFields = make([]octosql.VariableName, 0)
	for i := range firstRow {
		columnName := fmt.Sprintf("%s.col%d", rs.alias, i+1)
		rs.aliasedFields = append(rs.aliasedFields, octosql.VariableName(columnName))
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range firstRow {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(rs.aliasedFields, aliasedRecord)
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
			return rs.initializeColumnsWithoutHeaderRow(), nil
		}
	}

	if !rs.rows.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	row := extractRow(rs.rows.Columns(), rs.columnOffset)
	if len(row) != len(rs.aliasedFields) {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range row {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(rs.aliasedFields, aliasedRecord), nil
}

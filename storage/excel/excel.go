package excel

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

var excelInitialDate = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)

const hoursInDay = 24
const secondsInHour = 3600
const nanosecondsInSecond = 1000000000

func isCellNameValid(cell string) bool {
	cellNameRegexp := "^[A-Z]+[1-9][0-9]*$"

	r, _ := regexp.Compile(cellNameRegexp)

	return r.MatchString(cell)
}

func getRowAndColumnFromCell(cell string) (row, col string) {
	for index, char := range cell {
		if unicode.IsDigit(char) {
			return cell[index:], cell[:index]
		}
	}

	return "", cell
}

func getCoordinatesFromCell(cell string) (row int, col int, err error) {
	rowStr, colStr := getRowAndColumnFromCell(cell)
	col = excelize.TitleToNumber(colStr)

	rowTmp, err := strconv.ParseInt(rowStr, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrap(err, "couldn't parse row")
	}

	return int(rowTmp) - 1, col, nil
}

func isAllZs(cell string) bool {
	for _, char := range cell {
		if char != 'Z' {
			return false
		}
	}

	return true
}

func getNextColumn(cell string) string {
	if isAllZs(cell) {
		return strings.Repeat("A", len(cell)+1)
	}

	nextCol := ""

	index := len(cell)
	for i := len(cell) - 1; i >= 0; i-- {
		index--

		if cell[i] == 'Z' {
			nextCol += "A"
		} else {
			nextCol = string(cell[i]+1) + nextCol
			break
		}
	}

	return cell[:index] + nextCol
}

func createCellName(colName string, row int) string {
	return colName + strconv.Itoa(row)
}

func isDateStyle(style int) bool {
	return style == 2 || style == 3 || style == 4
}

func getDateFromExcelTime(timeValueStr string) (time.Time, error) {
	timeValue, err := strconv.ParseFloat(timeValueStr, 64)

	if err != nil {
		return time.Now(), errors.Wrap(err, "couldn't convert string to float")
	}

	daysPart := int64(timeValue)
	addedDays := excelInitialDate.Add(time.Hour * 24 * time.Duration(daysPart-2))

	timeLeft := timeValue - float64(daysPart) //fractions of day
	timeLeft *= nanosecondsInSecond * secondsInHour * hoursInDay

	fullDate := addedDays.Add(time.Nanosecond * time.Duration(timeLeft))

	return fullDate, nil
}

func padRow(row []string, targetLen int) []string {
	difference := targetLen - len(row)

	for i := 0; i < difference; i++ {
		row = append(row, "")
	}

	return row
}

func (rs *RecordStream) parseDataTypes(row []string) error {
	currentColumn := rs.rootColumnName
	for i, v := range row {
		currentCell := createCellName(currentColumn, rs.currentRowNumber)

		style := rs.file.GetCellStyle(rs.sheet, currentCell)

		if isDateStyle(style) {
			dateValue, err := getDateFromExcelTime(v)

			if err != nil {
				return errors.Wrap(err, "couldn't get date from excel time")
			}

			row[i] = dateValue.Format(time.RFC3339Nano)
		}

		currentColumn = getNextColumn(currentColumn)
	}

	return nil
}

func (rs *RecordStream) extractHeaderRow(row []string) ([]string, error) {
	resultRow := make([]string, 0)

	for i := rs.columnOffset; i < len(row); i++ {
		if row[i] != "" {
			resultRow = append(resultRow, row[i])
		} else {
			break
		}
	}

	err := rs.parseDataTypes(resultRow)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse types in row")
	}

	rs.currentRowNumber++

	return resultRow, nil
}

func (rs *RecordStream) extractStandardRow(row []string) ([]string, error) {
	columnCount := len(rs.aliasedFields)
	columnLimit := min(len(row), columnCount+rs.columnOffset)

	if len(row) <= rs.columnOffset {
		return make([]string, 0), nil
	}

	resultRow := padRow(row[rs.columnOffset:columnLimit], columnCount)

	err := rs.parseDataTypes(resultRow)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse types in row")
	}

	rs.currentRowNumber++

	return resultRow, nil
}

func (rs *RecordStream) initializeColumnsWithHeaderRow() error {
	columns, err := rs.extractHeaderRow(rs.rows.Columns())
	if err != nil {
		return errors.Wrap(err, "couldn't extract header row")
	}

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

func (rs *RecordStream) initializeColumnsWithoutHeaderRow() (*execution.Record, error) {
	firstRow, err := rs.extractHeaderRow(rs.rows.Columns())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract header row")
	}

	rs.aliasedFields = make([]octosql.VariableName, 0)
	for i := range firstRow {
		columnName := fmt.Sprintf("%s.col%d", rs.alias, i+1)
		rs.aliasedFields = append(rs.aliasedFields, octosql.VariableName(columnName))
	}

	data := make([]octosql.Value, 0)
	for _, v := range firstRow {
		data = append(data, execution.ParseType(v))
	}

	return execution.NewRecordFromSlice(rs.aliasedFields, data), nil
}

func isRowEmpty(row []string) bool {
	for i := range row {
		if row[i] != "" {
			return false
		}
	}

	return true
}

func min(a, b int) int {
	if a > b {
		return b
	}

	return a
}

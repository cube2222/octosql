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
	timeLeft *= float64((time.Hour * 24) / time.Nanosecond)

	fullDate := addedDays.Add(time.Nanosecond * time.Duration(timeLeft))

	return fullDate, nil
}

func padRow(row []string, targetLen int) []string {
	newRow := make([]string, len(row))
	copy(newRow, row)

	difference := targetLen - len(row)

	for i := 0; i < difference; i++ {
		newRow = append(newRow, "")
	}

	return newRow
}

func (rs *RecordStream) parseDataTypes(row []string) ([]octosql.Value, error) {
	currentColumn := rs.rootColumnName
	resultRow := make([]octosql.Value, len(row))

	for i, v := range row {
		currentCell := createCellName(currentColumn, rs.currentRowNumber)

		style := rs.file.GetCellStyle(rs.sheet, currentCell)

		if isDateStyle(style) {
			dateValue, err := getDateFromExcelTime(v)

			if err != nil {
				return nil, errors.Wrap(err, "couldn't get date from excel time")
			}

			resultRow[i] = octosql.MakeTime(dateValue)
		} else {
			resultRow[i] = execution.ParseType(row[i])
		}

		currentColumn = getNextColumn(currentColumn)
	}

	return resultRow, nil
}

/*
This function extracts column names from a row read by the excelize library.
It starts at the offset position and reads until first nil without parsing types.
For example when given a row: val1, val2, "", name, surname, "" and offset = 2,
then the extracted column names would be []{"name", "surname"}
*/
func (rs *RecordStream) extractColumnNamesFromRow(row []string) []string {
	stringRow := make([]string, 0)

	for i := rs.columnOffset; i < len(row); i++ {
		if row[i] != "" {
			stringRow = append(stringRow, row[i])
		} else {
			break
		}
	}

	rs.currentRowNumber++
	return stringRow
}

/*
This function does the same thing as the function that extracts column names,
but it parses data types. It is used to read the first row of a table
without a header row - it reads until the first nil, parses types and returns a slice of octosql.Value
*/
func (rs *RecordStream) extractRowUntilFirstNil(row []string) ([]octosql.Value, error) {
	stringRow := make([]string, 0)

	for i := rs.columnOffset; i < len(row); i++ {
		if row[i] != "" {
			stringRow = append(stringRow, row[i])
		} else {
			break
		}
	}

	resultRow, err := rs.parseDataTypes(stringRow)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse types in row")
	}

	rs.currentRowNumber++

	return resultRow, nil
}

/*
This function extracts a row from a full row read by the excelize library.
It reads exactly numberOfColumns values starting from the offset position,
and transforms every "" into a nil.
*/
func (rs *RecordStream) extractRow(row []string) ([]octosql.Value, error) {
	columnCount := len(rs.aliasedFields)
	columnLimit := min(len(row), columnCount+rs.columnOffset)

	if len(row) <= rs.columnOffset {
		return make([]octosql.Value, 0), nil
	}

	stringRow := padRow(row[rs.columnOffset:columnLimit], columnCount)

	resultRow, err := rs.parseDataTypes(stringRow)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse types in row")
	}

	rs.currentRowNumber++

	return resultRow, nil
}

func (rs *RecordStream) initializeColumnsWithHeaderRow() error {
	columns := rs.extractColumnNamesFromRow(rs.rows.Columns())

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
	firstRow, err := rs.extractRowUntilFirstNil(rs.rows.Columns())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract header row")
	}

	rs.aliasedFields = make([]octosql.VariableName, 0)
	for i := range firstRow {
		columnName := fmt.Sprintf("%s.col%d", rs.alias, i+1)
		rs.aliasedFields = append(rs.aliasedFields, octosql.VariableName(columnName))
	}

	return execution.NewRecordFromSlice(rs.aliasedFields, firstRow), nil
}

func isRowEmpty(row []octosql.Value) bool {
	for i := range row {
		if row[i] != octosql.ZeroString() {
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

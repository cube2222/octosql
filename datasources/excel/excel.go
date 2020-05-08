package excel

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

var cellRegexp = regexp.MustCompile("^[A-Z]+[1-9][0-9]*$")

func getCoordinatesFromCell(cell string) (row int, col int, err error) {
	if !cellRegexp.MatchString(cell) {
		return 0, 0, errors.Wrap(err, "invalid cell")
	}

	rowStr, colStr := getRowAndColumnFromCell(cell)
	col = excelize.TitleToNumber(colStr)

	rowTmp, err := strconv.ParseInt(rowStr, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrap(err, "couldn't parse row")
	}

	return int(rowTmp) - 1, col, nil
}

func getRowAndColumnFromCell(cell string) (row, col string) {
	for index, char := range cell {
		if unicode.IsDigit(char) {
			return cell[index:], cell[:index]
		}
	}

	return "", cell
}

func getHeaderRow(datasourceAlias string, horizontalOffset int, row []string) ([]octosql.VariableName, error) {
	rawColumnNames := readRowNoGaps(horizontalOffset, row)

	columnNames := make([]octosql.VariableName, len(rawColumnNames))
	for i, c := range rawColumnNames {
		lowerCased := strings.ToLower(fmt.Sprintf("%s.%s", datasourceAlias, c))
		columnNames[i] = octosql.NewVariableName(lowerCased)
	}

	set := make(map[octosql.VariableName]struct{})
	for _, col := range columnNames {
		if _, present := set[col]; present {
			return nil, errors.New("column names not unique")
		}
		set[col] = struct{}{}
	}

	return columnNames, nil
}

func generatePlaceholders(datasourceAlias string, horizontalOffset int, row []string) ([]octosql.VariableName, error) {
	firstRow := readRowNoGaps(horizontalOffset, row)

	columnNames := make([]octosql.VariableName, len(firstRow))
	for i := range firstRow {
		columnName := fmt.Sprintf("%s.col%d", datasourceAlias, i+1)
		columnNames[i] = octosql.NewVariableName(columnName)
	}

	return columnNames, nil
}

/*
readRowNoGaps extracts strings from a row read by the excelize library.
It starts at the offset position and reads until first nil without parsing types.
For example when given a row: val1, val2, "", name, surname, "" and offset = 2,
then the extracted strings would be []{"name", "surname"}
*/
func readRowNoGaps(horizontalOffset int, row []string) []string {
	stringRow := make([]string, 0)

	for i := horizontalOffset; i < len(row); i++ {
		if row[i] != "" {
			stringRow = append(stringRow, row[i])
		} else {
			break
		}
	}

	return stringRow
}

/*
This function extracts a row from a full row read by the excelize library.
It reads exactly numberOfColumns values starting from the offset position,
and transforms every "" into a nil.
*/
func (rs *RecordStream) extractRow(row []string) ([]octosql.Value, error) {
	columnCount := len(rs.columnNames)
	columnLimit := min(len(row), columnCount+rs.horizontalOffset)

	if len(row) <= rs.horizontalOffset {
		return make([]octosql.Value, 0), nil
	}

	stringRow := padRow(row[rs.horizontalOffset:columnLimit], columnCount)

	resultRow, err := rs.parseDataTypes(stringRow)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse types in row")
	}

	return resultRow, nil
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
	resultRow := make([]octosql.Value, len(row))

	for i, v := range row {
		if v == "" {
			resultRow[i] = octosql.MakeNull()
			continue
		}
		if rs.timeColumns[i] {
			dateValue, err := getDateFromExcelTime(v)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get date from excel time")
			}

			resultRow[i] = octosql.MakeTime(dateValue)
		} else {
			resultRow[i] = execution.ParseType(row[i])
		}
	}

	return resultRow, nil
}

var excelInitialDate = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)

func getDateFromExcelTime(timeValueStr string) (time.Time, error) {
	timeValue, err := strconv.ParseFloat(timeValueStr, 64)
	if err != nil {
		return time.Now(), errors.Wrap(err, "couldn't convert string to float")
	}

	daysPart := int64(timeValue)
	addedDays := excelInitialDate.Add(time.Hour * 24 * time.Duration(daysPart-2))

	timeLeft := timeValue - float64(daysPart) //fractions of day
	timeLeft *= float64(time.Hour * 24 / time.Millisecond)

	fullDate := addedDays.Add(time.Millisecond * time.Duration(timeLeft))

	return fullDate.Round(time.Second), nil
}

func isRowEmpty(row []octosql.Value) bool {
	for i := range row {
		if !octosql.AreEqual(row[i], octosql.MakeNull()) {
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

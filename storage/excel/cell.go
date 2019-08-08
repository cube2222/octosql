package excel

import (
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"unicode"
)

var ErroneousCell = newCell("_", 0)

type cell struct {
	column string
	row    int
}

func newCell(col string, row int) cell {
	return cell{
		column: col,
		row:    row,
	}
}

func (cell cell) getCellName() string {
	return cell.column + strconv.Itoa(cell.row)
}

func isAllZs(columnName string) bool {
	for _, char := range columnName {
		if char != 'Z' {
			return false
		}
	}

	return true
}

func (cell cell) getCellToTheRight() cell {
	columnName := cell.column

	if isAllZs(columnName) {
		newColumnName := strings.Repeat("A", len(columnName)+1)
		return newCell(newColumnName, cell.row)
	}

	index := len(columnName)

	newColumnName := ""

	for i := len(columnName) - 1; i >= 0; i-- {
		index--

		if columnName[i] == 'Z' {
			newColumnName += "A"
		} else {
			newColumnName = string(columnName[i]+1) + newColumnName
			break
		}
	}

	newColumnName = columnName[:index] + newColumnName

	return newCell(newColumnName, cell.row)
}

func (cell cell) getCellBelow() cell {
	return newCell(cell.column, cell.row+1)
}

func isCorrectChar(char rune) bool {
	return unicode.IsUpper(char) || unicode.IsDigit(char)
}

func getCellFromName(cell string) (cell, error) {

	for index, char := range cell {
		if !isCorrectChar(char) {
			return ErroneousCell, errors.Errorf("The character %v can't make up for column name", char)
		}

		if unicode.IsDigit(char) {
			if index == 0 {
				return ErroneousCell, errors.Errorf("The first char of a cell can't be a digit")
			}

			columnName := cell[:index]
			row := cell[index:]

			rowNumeric, err := strconv.Atoi(row)
			if err != nil {
				return ErroneousCell, errors.Wrap(err, "couldn't parse row of cell")
			}

			return newCell(columnName, rowNumeric), nil
		}
	}

	return ErroneousCell, errors.Errorf("cell %s has no row number", cell)
}

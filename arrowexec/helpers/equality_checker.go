package helpers

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

func MakeRowEqualityChecker(leftKeys, rightKeys []arrow.Array) func(leftRowIndex, rightRowIndex int) bool {
	if len(leftKeys) != len(rightKeys) {
		panic(fmt.Errorf("key column count mismatch in equality checker: %d != %d", len(leftKeys), len(rightKeys)))
	}
	keyColumnCount := len(leftKeys)

	columnEqualityCheckers := make([]func(leftRowIndex, rightRowIndex int) bool, keyColumnCount)
	for i := 0; i < keyColumnCount; i++ {
		switch leftKeys[i].DataType().ID() {
		case arrow.INT64:
			// TODO: Handle nulls.
			leftTypedArr := leftKeys[i].(*array.Int64).Int64Values()
			rightTypedArr := rightKeys[i].(*array.Int64).Int64Values()
			columnEqualityCheckers[i] = func(leftRowIndex, rightRowIndex int) bool {
				return leftTypedArr[leftRowIndex] == rightTypedArr[rightRowIndex]
			}
		case arrow.FLOAT64:
			leftTypedArr := leftKeys[i].(*array.Float64).Float64Values()
			rightTypedArr := rightKeys[i].(*array.Float64).Float64Values()
			columnEqualityCheckers[i] = func(leftRowIndex, rightRowIndex int) bool {
				return leftTypedArr[leftRowIndex] == rightTypedArr[rightRowIndex]
			}
		case arrow.STRING:
			// TODO: Move to large string array.
			leftTypedArr := leftKeys[i].(*array.String)
			rightTypedArr := rightKeys[i].(*array.String)
			columnEqualityCheckers[i] = func(leftRowIndex, rightRowIndex int) bool {
				return leftTypedArr.Value(leftRowIndex) == rightTypedArr.Value(rightRowIndex)
			}
			// TODO: Handle all types.
		default:
			panic("unsupported type for equality checker")
		}
	}

	return func(leftRowIndex, rightRowIndex int) bool {
		for i := 0; i < keyColumnCount; i++ {
			if !columnEqualityCheckers[i](leftRowIndex, rightRowIndex) {
				return false
			}
		}
		return true
	}
}

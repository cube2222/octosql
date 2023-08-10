package helpers

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cube2222/octosql/arrowexec/execution"
)

func MakeKeyEqualityChecker(leftRecord, rightRecord execution.Record, leftKeyIndices, rightKeyIndices []int) func(leftRowIndex, rightRowIndex int) bool {
	if len(leftKeyIndices) != len(rightKeyIndices) {
		panic("key column count mismatch in equality checker")
	}
	keyColumnCount := len(leftKeyIndices)

	columnEqualityCheckers := make([]func(leftRowIndex, rightRowIndex int) bool, keyColumnCount)
	for i := 0; i < keyColumnCount; i++ {
		switch leftRecord.Column(leftKeyIndices[i]).DataType().ID() {
		case arrow.INT64:
			// TODO: Handle nulls.
			leftTypedArr := leftRecord.Column(leftKeyIndices[i]).(*array.Int64).Int64Values()
			rightTypedArr := rightRecord.Column(rightKeyIndices[i]).(*array.Int64).Int64Values()
			columnEqualityCheckers[i] = func(leftRowIndex, rightRowIndex int) bool {
				return leftTypedArr[leftRowIndex] == rightTypedArr[rightRowIndex]
			}
		case arrow.FLOAT64:
			leftTypedArr := leftRecord.Column(leftKeyIndices[i]).(*array.Float64).Float64Values()
			rightTypedArr := rightRecord.Column(rightKeyIndices[i]).(*array.Float64).Float64Values()
			columnEqualityCheckers[i] = func(leftRowIndex, rightRowIndex int) bool {
				return leftTypedArr[leftRowIndex] == rightTypedArr[rightRowIndex]
			}
		case arrow.STRING:
			// TODO: Move to large string array.
			leftTypedArr := leftRecord.Column(leftKeyIndices[i]).(*array.String)
			rightTypedArr := rightRecord.Column(rightKeyIndices[i]).(*array.String)
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

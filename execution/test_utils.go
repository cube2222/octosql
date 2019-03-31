package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

//this function assumes that the names are sorted
func areSlicesOfVariableNamesEqual(first, second []octosql.VariableName) bool {
	if len(first) != len(second) {
		return false
	}

	for i := range first {
		if first[i] != second[i] {
			return false
		}
	}

	return true
}

func AreRecordsEqual(first, second *Record) bool {
	if len(first.fieldNames) != len(second.fieldNames) {
		return false
	}

	for i := range first.fieldNames {
		colName := first.fieldNames[i]
		f := first.Value(colName)
		s := second.Value(colName)
		if !AreEqual(f, s) {
			return false
		}
	}

	return true
}

func AreStreamsEqual(first, second RecordStream) (bool, error) {
	for {
		firstRec, firstErr := first.Next()
		secondRec, secondErr := second.Next()

		if firstErr == ErrEndOfStream && secondErr == ErrEndOfStream {
			return true, nil
		}

		if (firstErr == ErrEndOfStream && secondErr == nil) ||
			(firstErr == nil && secondErr == ErrEndOfStream) {
			return false, nil
		}

		if firstErr != nil {
			return false, errors.Wrap(firstErr, "something went wrong in first stream")
		}

		if secondErr != nil {
			return false, errors.Wrap(secondErr, "something went wrong in second stream")
		}

		if !AreRecordsEqual(firstRec, secondRec) {
			return false, nil
		}
	}
}

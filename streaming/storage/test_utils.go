package storage

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

func TestDequeueIterator(iter *DequeueIterator, expectedValues []octosql.Value) (bool, error) {
	var value octosql.Value

	for i := 0; i < len(expectedValues); i++ {
		err := iter.Next(&value)

		if err != nil {
			return false, errors.Wrap(err, "expected a value, got an error")
		}

		if !octosql.AreEqual(value, expectedValues[i]) {
			return false, errors.Errorf("mismatch of values at index %d", i)
		}
	}

	err := iter.Next(&value)
	if err != ErrEndOfIterator {
		return false, errors.New("expected ErrEndOfIterator")
	}

	return true, nil
}

func TestMapIteratorCorrectness(iter *MapIterator, expectedKeys, expectedValues []octosql.Value, reverse bool) (bool, error) {
	var key octosql.Value
	var value octosql.Value

	length := len(expectedValues)

	for i := 0; i < length; i++ {
		err := iter.Next(&key, &value)

		if err != nil {
			return false, errors.Wrap(err, "expected a value, got an error")
		}

		if reverse {
			if !octosql.AreEqual(value, expectedValues[length-1-i]) || !octosql.AreEqual(key, expectedKeys[length-1-i]) {
				return false, errors.Errorf("mismatch of values at index %d", i)
			}
		} else {
			if !octosql.AreEqual(value, expectedValues[i]) || !octosql.AreEqual(key, expectedKeys[i]) {
				return false, errors.Errorf("mismatch of values at index %d", i)
			}
		}
	}

	err := iter.Next(&key, &value)
	if err != ErrEndOfIterator {
		return false, errors.New("expected ErrEndOfIterator")
	}

	return true, nil
}

//The iterator of a set has no determined order
func TestSetIteratorCorrectness(iter *SetIterator, expectedValues []octosql.Value) (bool, error) {
	var value octosql.Value

	seen := make([]bool, len(expectedValues))

	for i := 0; i < len(expectedValues); i++ {
		err := iter.Next(&value)

		if err == ErrEndOfIterator {
			return false, nil
		} else if err != nil {
			return false, err
		}

		index := getPositionInTuple(expectedValues, value)
		if seen[index] {
			return false, errors.New("the set contains the same value twice")
		}

		seen[index] = true
	}

	err := iter.Next(&value)
	if err != ErrEndOfIterator {
		return false, errors.New("the iterator should've ended, but it didn't")
	}

	return true, nil
}

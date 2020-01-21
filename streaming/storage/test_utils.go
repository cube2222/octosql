package storage

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

func TestDequeIterator(iter *DequeIterator, expectedValues []octosql.Value) (bool, error) {
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

func TestMapIteratorCorrectness(iter *MapIterator, expectedKeys, expectedValues []octosql.Value) (bool, error) {
	var key octosql.Value
	var value octosql.Value

	length := len(expectedValues)

	for i := 0; i < length; i++ {
		err := iter.Next(&key, &value)

		if err != nil {
			return false, errors.Wrap(err, "expected a value, got an error")
		}

		if !octosql.AreEqual(value, expectedValues[i]) || !octosql.AreEqual(key, expectedKeys[i]) {
			return false, errors.Errorf("mismatch of values at index %d", i)
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

func reverseValues(values []octosql.Value) []octosql.Value {
	length := len(values)
	result := make([]octosql.Value, length)

	for i := 0; i < length; i++ {
		result[length-i-1] = values[i]
	}

	return result
}

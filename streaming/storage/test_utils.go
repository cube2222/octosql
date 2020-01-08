package storage

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

func TestLinkedListIterator(iter *LinkedListIterator, expectedValues []octosql.Value) (bool, error) {
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

	for i := 0; i < len(expectedValues); i++ {
		err := iter.Next(&key, &value)

		if err != nil {
			return false, errors.Wrap(err, "expected a value, got an error")
		}

		if !octosql.AreEqual(value, expectedValues[i]) {
			return false, errors.Errorf("mismatch of values at index %d", i)
		}

		if !octosql.AreEqual(key, expectedKeys[i]) {
			return false, errors.Errorf("mismatch of keys at index %d", i)
		}
	}

	err := iter.Next(&key, &value)
	if err != ErrEndOfIterator {
		return false, errors.New("expected ErrEndOfIterator")
	}

	return true, nil
}

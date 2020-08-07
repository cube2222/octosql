package storage

import (
	cryptorand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/oklog/ulid"

	"github.com/cube2222/octosql"

	"github.com/dgraph-io/badger/v2"
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

func reverseValues(values []octosql.Value) []octosql.Value {
	length := len(values)
	result := make([]octosql.Value, length)

	for i := 0; i < length; i++ {
		result[length-i-1] = values[i]
	}

	return result
}

var globalTestStorage *badger.DB
var globalTestStorageInitializer = sync.Once{}

func GetTestStorage(t *testing.T) Storage {
	globalTestStorageInitializer.Do(func() {
		dirname := fmt.Sprintf("testdb/%d", rand.Int())
		err := os.MkdirAll(dirname, os.ModePerm)
		if err != nil {
			t.Fatal("couldn't create temporary directory: ", err)
		}

		opts := badger.DefaultOptions(dirname)
		if runtime.GOOS == "windows" { // TODO - fix while refactoring config
			opts = opts.WithValueLogLoadingMode(options.FileIO)
		}

		db, err := badger.Open(opts)
		if err != nil {
			t.Fatal("couldn't open in-memory badger database: ", err)
		}
		globalTestStorage = db
	})
	prefix := ulid.MustNew(ulid.Timestamp(time.Now()), cryptorand.Reader).String()

	return NewBadgerStorage(globalTestStorage).WithPrefix([]byte(prefix + "$"))
}

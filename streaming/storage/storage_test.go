package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/stretchr/testify/assert"
)

func TestBadgerStorage_Subscribe(t *testing.T) {
	ctx := context.Background()
	prefix := []byte{'a'}

	// This key should be printed, since it matches the prefix.
	aKey := []byte("a-key")
	aValue := []byte("a-value")

	// This key should not be printed.
	bKey := []byte("b-key")
	bValue := []byte("b-value")

	opts := badger.DefaultOptions("")
	opts.Dir = ""
	opts.ValueDir = ""
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := NewBadgerStorage(db).WithPrefix(prefix)
	subscription := storage.Subscribe(ctx)

	// Write both keys, but only one should be printed in the Output.
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(aKey, aValue) })
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(bKey, bValue) })
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = subscription.ListenForChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = subscription.ListenForChanges(ctx)
	if err != context.DeadlineExceeded {
		t.Fatal(err)
	}

	err = subscription.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBadgerStorage_DropAll(t *testing.T) {
	prefixA := []byte{'A'}
	prefixB := []byte{'B'}
	prefixC := []byte{'C'}

	opts := badger.DefaultOptions("")
	opts.Dir = ""
	opts.ValueDir = ""
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := NewBadgerStorage(db)
	tx := storage.BeginTransaction()

	// Inserting values
	if err := tx.WithPrefix(prefixA).Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := tx.WithPrefix(prefixA).Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	if err := tx.WithPrefix(prefixA).Set([]byte("key3"), []byte("value3")); err != nil {
		t.Fatal(err)
	}

	if err := tx.WithPrefix(prefixB).Set([]byte("key4"), []byte("value4")); err != nil {
		t.Fatal(err)
	}
	if err := tx.WithPrefix(prefixB).Set([]byte("key5"), []byte("value5")); err != nil {
		t.Fatal(err)
	}

	if err := tx.WithPrefix(prefixC).Set([]byte("key6"), []byte("value6")); err != nil {
		t.Fatal(err)
	}

	// Checking A values
	for i := 1; i <= 3; i++ {
		value, err := tx.WithPrefix(prefixA).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}

	// Checking B values
	for i := 4; i <= 5; i++ {
		value, err := tx.WithPrefix(prefixB).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}

	// Checking C values
	for i := 6; i <= 6; i++ {
		value, err := tx.WithPrefix(prefixC).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}
	tx.Commit()

	// Dropping prefix A and B
	if err := storage.DropAll(prefixA); err != nil {
		t.Fatal(err)
	}
	if err := storage.DropAll(prefixB); err != nil {
		t.Fatal(err)
	}

	tx = storage.BeginTransaction()
	// Checking A values
	for i := 1; i <= 3; i++ {
		_, err := tx.WithPrefix(prefixA).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	// Checking B values
	for i := 4; i <= 5; i++ {
		_, err := tx.WithPrefix(prefixB).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	// Checking C values
	for i := 6; i <= 6; i++ {
		value, err := tx.WithPrefix(prefixC).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}
	tx.Commit()

	// Dropping prefix C
	if err := storage.DropAll(prefixC); err != nil {
		t.Fatal(err)
	}

	tx = storage.BeginTransaction()
	// Checking A values
	for i := 1; i <= 3; i++ {
		_, err := tx.WithPrefix(prefixA).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	// Checking B values
	for i := 4; i <= 5; i++ {
		_, err := tx.WithPrefix(prefixB).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	// Checking C values
	for i := 4; i <= 5; i++ {
		_, err := tx.WithPrefix(prefixC).Get([]byte(fmt.Sprintf("key%d", i)))
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}
	tx.Commit()
}

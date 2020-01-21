package storage

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
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

	log.Printf("stopping subscription")
	err = subscription.Close()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("waiting for subscription to close")
}

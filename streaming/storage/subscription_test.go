package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
)

func TestConcatSubscriptions(t *testing.T) {
	ctx := context.Background()
	aPrefix := []byte{'a'}
	bPrefix := []byte{'b'}

	// This key should be printed, since it matches the prefix.
	aKey := []byte("a-key")
	aValue := []byte("a-value")

	// This key should be printed, since it matches the prefix.
	bKey := []byte("b-key")
	bValue := []byte("b-value")

	// This key should not be printed.
	cKey := []byte("c-key")
	cValue := []byte("c-value")

	dirname := fmt.Sprintf("testdb/%d", rand.Int())
	err := os.MkdirAll(dirname, os.ModePerm)
	if err != nil {
		t.Fatal("couldn't create temporary directory: ", err)
	}

	opts := badger.DefaultOptions(dirname)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := NewBadgerStorage(db)
	aSubscription := storage.WithPrefix(aPrefix).Subscribe(ctx)
	bSubscription := storage.WithPrefix(bPrefix).Subscribe(ctx)
	concatted := ConcatSubscriptions(ctx, aSubscription, bSubscription)

	// Write both keys, but only one should be printed in the Output.
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(aKey, aValue) })
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(bKey, bValue) })
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(cKey, cValue) })
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = concatted.ListenForChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = concatted.ListenForChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = concatted.ListenForChanges(ctx)
	if err != context.DeadlineExceeded {
		t.Fatal(err)
	}

	err = concatted.Close()
	if err != nil {
		t.Fatal(err)
	}
}

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

func TestBadgerStorage_Subscribe(t *testing.T) {
	ctx := context.Background()

	aKey := []byte("a-key")
	aValue := []byte("a-value")

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
	subscription := storage.Subscribe(ctx)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = subscription.ListenForChanges(ctx)
	if err != context.DeadlineExceeded {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error { return txn.Set(aKey, aValue) })
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	err = subscription.ListenForChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = subscription.Close()
	if err != nil {
		t.Fatal(err)
	}
}

package storage

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
)

func TestBadgerStorage_Subscribe(t *testing.T) {
	prefix := []byte{'a'}

	// This key should be printed, since it matches the prefix.
	aKey := []byte("a-key")
	aValue := []byte("a-value")

	// This key should not be printed.
	bKey := []byte("b-key")
	bValue := []byte("b-value")

	// Open the DB.
	dir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	storage := NewBadgerStorage(db).WithPrefix(prefix)

	// Create the context here so we can cancel it after sending the writes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use the WaitGroup to make sure we wait for the subscription to stop before continuing.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cb := func() error {
			println("Received")
			return nil
		}

		if err := storage.Subscribe(ctx, cb); err != nil && err != context.Canceled {
			panic(err)
		}
		log.Printf("subscription closed")
	}()

	// Wait for the above go routine to be scheduled.
	time.Sleep(2 * time.Second)
	// Write both keys, but only one should be printed in the Output.
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(aKey, aValue) })
	if err != nil {
		panic(err)
	}
	err = db.Update(func(txn *badger.Txn) error { return txn.Set(bKey, bValue) })
	if err != nil {
		panic(err)
	}

	log.Printf("stopping subscription")
	cancel()
	log.Printf("waiting for subscription to close")
	wg.Wait()
}

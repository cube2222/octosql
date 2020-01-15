package storage

import (
	"context"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
)

type KeyValueList = *badger.KVList

type Storage interface {
	DropAll(prefix []byte) error
	BeginTransaction() StateTransaction
	WithPrefix(prefix []byte) Storage
	Subscribe(ctx context.Context, onNotification func() error) error
}

type BadgerStorage struct {
	db     *badger.DB
	prefix []byte
}

func NewBadgerStorage(db *badger.DB) *BadgerStorage {
	return &BadgerStorage{
		db: db,
	}
}

func (bs *BadgerStorage) BeginTransaction() StateTransaction {
	//bs.db.DropPrefix()
	tx := bs.db.NewTransaction(true)
	return &badgerTransaction{tx: tx, prefix: bs.prefix}
}

func (bs *BadgerStorage) DropAll(prefix []byte) error {
	err := bs.db.DropPrefix(prefix)
	return err
}

func (bs *BadgerStorage) WithPrefix(prefix []byte) Storage {
	copyStorage := *bs
	copyStorage.prefix = append(copyStorage.prefix, prefix...)

	return &copyStorage
}

func (bs *BadgerStorage) Subscribe(ctx context.Context, onNotification func() error) error {
	cb := func(kv KeyValueList) error {
		return onNotification()
	}

	err := bs.db.Subscribe(ctx, cb, bs.prefix)
	if err != nil {
		return errors.Wrap(err, "couldn't subscribe to db")
	}

	return nil
}

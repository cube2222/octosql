package storage

import (
	"context"

	"github.com/dgraph-io/badger/v2"
)

type Storage interface {
	DropAll(prefix []byte) error
	BeginTransaction() StateTransaction
	WithPrefix(prefix []byte) Storage
	Subscribe(ctx context.Context) *Subscription
	Close() error
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
	tx := bs.db.NewTransaction(true)
	return &badgerTransaction{tx: tx, prefix: bs.prefix, storage: bs}
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

func (bs *BadgerStorage) Subscribe(ctx context.Context) *Subscription {
	return NewSubscription(ctx, func(ctx context.Context, changes chan<- struct{}) error {
		return bs.db.Subscribe(ctx, func(kv *badger.KVList) error {
			select {
			case changes <- struct{}{}:
				return ErrChangeSent
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}, bs.prefix)
	})
}

func (bs *BadgerStorage) Close() error {
	return bs.db.Close()
}

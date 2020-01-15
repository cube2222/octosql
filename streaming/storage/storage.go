package storage

import "github.com/dgraph-io/badger/v2"

type Storage interface {
	DropAll(prefix []byte) error
	BeginTransaction() StateTransaction
	WithPrefix(prefix []byte) StateTransaction
}

type BadgerStorage struct {
	db *badger.DB
}

func NewBadgerStorage(db *badger.DB) *BadgerStorage {
	return &BadgerStorage{
		db: db,
	}
}

func (bs *BadgerStorage) BeginTransaction() StateTransaction {
	//bs.db.DropPrefix()
	tx := bs.db.NewTransaction(true)
	return &badgerTransaction{tx: tx, prefix: nil}
}

func (bs *BadgerStorage) DropAll(prefix []byte) error {
	err := bs.db.DropPrefix(prefix)
	return err
}

func (bs *BadgerStorage) WithPrefix(prefix []byte) StateTransaction {
	tx := bs.db.NewTransaction(true)
	badgerT := &badgerTransaction{tx: tx, prefix: nil}
	return badgerT.WithPrefix(prefix)
}

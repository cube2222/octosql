package storage

import (
	"bytes"
	"context"

	"github.com/dgraph-io/badger/v2"
)

type Storage interface {
	DropAll(prefix []byte) error
}

type StateTransaction interface {
	Set(key, value []byte) error
	Get(key []byte) (value []byte, err error)
	Delete(key []byte) error
	WithPrefix(prefix []byte) StateTransaction
	GetPrefixLength() int
	Iterator(opts badger.IteratorOptions) Iterator //TODO: opts should be some other structure/interface
	Commit() error
	Abort()
}

type stateTransactionKey struct{}

// For now save in context
func GetStateTransactionFromContext(ctx context.Context) StateTransaction {
	tx, ok := ctx.Value(stateTransactionKey{}).(StateTransaction)
	if !ok {
		panic("no storage transaction in context")
	}
	return tx
}

// For now save in context
func InjectStateTransaction(ctx context.Context, tx StateTransaction) context.Context {
	return context.WithValue(ctx, stateTransactionKey{}, tx)
}

type BadgerStorage struct {
	db *badger.DB
}

func NewBadgerStorage(db *badger.DB) *BadgerStorage {
	return &BadgerStorage{
		db: db,
	}
}

func (bs *BadgerStorage) BeginTransaction() *badgerTransaction {
	//bs.db.DropPrefix()
	tx := bs.db.NewTransaction(true)
	return &badgerTransaction{tx: tx, prefix: nil}
}

//TODO: What to do? What to do?
func (bs *BadgerStorage) DropAll(prefix []byte) error {
	err := bs.db.DropPrefix(prefix)
	return err
}

type badgerTransaction struct {
	tx     *badger.Txn
	prefix []byte
}

func (tx *badgerTransaction) getKeyWithPrefix(key []byte) []byte {
	var buf bytes.Buffer
	buf.Write(tx.prefix)
	buf.Write(key)
	return buf.Bytes()
}

func (tx *badgerTransaction) Set(key, value []byte) error {
	return tx.tx.Set(tx.getKeyWithPrefix(key), value)
}

func (tx *badgerTransaction) Get(key []byte) ([]byte, error) {
	var value []byte
	item, err := tx.tx.Get(tx.getKeyWithPrefix(key))
	if err != nil {
		return nil, err
	}
	value, err = item.ValueCopy(value)
	if err != nil {
		return nil, err
	}
	return value, err
}

func (tx *badgerTransaction) Delete(key []byte) error {
	return tx.tx.Delete(tx.getKeyWithPrefix(key))
}

func (tx *badgerTransaction) GetPrefixLength() int {
	return len(tx.prefix)
}

func (tx *badgerTransaction) WithPrefix(prefix []byte) StateTransaction {
	return &badgerTransaction{
		tx:     tx.tx,
		prefix: tx.getKeyWithPrefix(prefix),
	}
}

func (tx *badgerTransaction) Iterator(opts badger.IteratorOptions) Iterator {
	opts.Prefix = tx.getKeyWithPrefix(opts.Prefix)
	it := tx.tx.NewIterator(opts)
	return NewBadgerIterator(it, tx.GetPrefixLength())
}

func (tx *badgerTransaction) Commit() error {
	return tx.tx.Commit()
}

func (tx *badgerTransaction) Abort() {
	tx.tx.Discard()
}

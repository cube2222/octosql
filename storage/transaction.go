package storage

import (
	"bytes"
	"log"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type StateTransaction interface {
	Set(key, value []byte) error
	Get(key []byte) (value []byte, err error)
	Delete(key []byte) error
	WithPrefix(prefix []byte) StateTransaction
	GetPrefixLength() int
	Iterator(opts ...IteratorOption) Iterator
	Commit() error
	Abort()
	GetUnderlyingStorage() Storage
	Prefix() string
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

type badgerTransaction struct {
	prefix  []byte
	storage *BadgerStorage
}

func (tx *badgerTransaction) getKeyWithPrefix(key []byte) []byte {
	var buf bytes.Buffer
	buf.Write(tx.prefix)
	buf.Write(key)
	return buf.Bytes()
}

func (tx *badgerTransaction) Set(key, value []byte) error {
	badgerTx := tx.storage.db.NewTransaction(true)
	if err := badgerTx.Set(tx.getKeyWithPrefix(key), value); err != nil {
		return err
	}
	return badgerTx.Commit()
}

func (tx *badgerTransaction) Get(key []byte) ([]byte, error) {
	var value []byte

	badgerTx := tx.storage.db.NewTransaction(false)

	item, err := badgerTx.Get(tx.getKeyWithPrefix(key))
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get byte key")
	}

	value, err = item.ValueCopy(value)
	if err != nil {
		return nil, err
	}

	if err := badgerTx.Commit(); err != nil {
		return nil, err
	}
	return value, nil
}

func (tx *badgerTransaction) Delete(key []byte) error {
	badgerTx := tx.storage.db.NewTransaction(true)
	if err := badgerTx.Delete(tx.getKeyWithPrefix(key)); err != nil {
		return err
	}
	return badgerTx.Commit()
}

func (tx *badgerTransaction) GetPrefixLength() int {
	return len(tx.prefix)
}

func (tx *badgerTransaction) WithPrefix(prefix []byte) StateTransaction {
	return &badgerTransaction{
		prefix:  tx.getKeyWithPrefix(prefix),
		storage: tx.storage.WithPrefix(prefix).(*BadgerStorage),
	}
}

func (tx *badgerTransaction) Prefix() string {
	return string(tx.prefix)
}

func (tx *badgerTransaction) Iterator(opts ...IteratorOption) Iterator {
	options := &IteratorOptions{
		Prefix: tx.prefix,
	}

	badgerTx := tx.storage.db.NewTransaction(false)

	for _, opt := range opts {
		opt(options)
	}

	it := badgerTx.NewIterator(options.ToBadgerOptions())

	if options.Reverse {
		it.Seek(append(options.Prefix, 255))
	} else {
		it.Rewind()
	}

	return NewBadgerIterator(tx, it, tx.GetPrefixLength())
}

func (tx *badgerTransaction) Commit() error {
	return nil
}

func (tx *badgerTransaction) Abort() {
	log.Fatalf("%+v", errors.New("abort called"))
	return
}

func (tx *badgerTransaction) GetUnderlyingStorage() Storage {
	return tx.storage
}

//IteratorOptions are a copy of badger.IteratorOptions
//They are used so that there is no explicit badger dependency in StateTransaction
type IteratorOptions struct {
	PrefetchValues bool
	PrefetchSize   int
	Reverse        bool
	AllVersions    bool
	Prefix         []byte
	InternalAccess bool
}

func (io *IteratorOptions) ToBadgerOptions() badger.IteratorOptions {
	return badger.IteratorOptions{
		PrefetchValues: io.PrefetchValues,
		PrefetchSize:   io.PrefetchSize,
		Reverse:        io.Reverse,
		AllVersions:    io.AllVersions,
		Prefix:         io.Prefix,
		InternalAccess: io.InternalAccess,
	}
}

type IteratorOption func(*IteratorOptions)

func WithPrefix(prefix []byte) IteratorOption {
	return func(opts *IteratorOptions) {
		opts.Prefix = append(opts.Prefix, prefix...)
	}
}

func WithDefault() IteratorOption {
	return func(opts *IteratorOptions) {
		opts.Reverse = false
		opts.AllVersions = false
	}
}

func WithReverse() IteratorOption {
	return func(opts *IteratorOptions) {
		opts.Reverse = true
	}
}

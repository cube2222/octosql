package storage

import (
	"bytes"

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
	tx      *badger.Txn
	prefix  []byte
	storage Storage
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
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get byte key")
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
		tx:      tx.tx,
		prefix:  tx.getKeyWithPrefix(prefix),
		storage: tx.storage.WithPrefix(prefix),
	}
}

func (tx *badgerTransaction) Prefix() string {
	return string(tx.prefix)
}

func (tx *badgerTransaction) Iterator(opts ...IteratorOption) Iterator {
	options := &IteratorOptions{
		Prefix: tx.prefix,
	}

	for _, opt := range opts {
		opt(options)
	}

	it := tx.tx.NewIterator(options.ToBadgerOptions())

	if options.Reverse {
		it.Seek(append(options.Prefix, 255))
	} else {
		it.Rewind()
	}

	return NewBadgerIterator(it, tx.GetPrefixLength())
}

func (tx *badgerTransaction) Commit() error {
	return tx.tx.Commit()
}

func (tx *badgerTransaction) Abort() {
	tx.tx.Discard()
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

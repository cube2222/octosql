package storage

import (
	"io"

	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var ErrEndOfIterator = errors.New("end of iterator")

type Iterator interface {
	Next(proto.Message) error
	NextWithKey(key MonotonicallySerializable, value proto.Message) error
	io.Closer
}

//BadgerIterator is a wrapper around *badger.Iterator that implements
//the Iterator interface
type BadgerIterator struct {
	it           *badger.Iterator
	prefixLength int
}

func NewBadgerIterator(it *badger.Iterator, prefixLength int) *BadgerIterator {
	return &BadgerIterator{
		it:           it,
		prefixLength: prefixLength,
	}
}

func (bi *BadgerIterator) NextWithKey(key MonotonicallySerializable, value proto.Message) error {
	err := bi.currentKey(key)
	if err != nil {
		return err
	}

	err = bi.currentValue(value)
	if err != nil {
		return err
	}

	bi.it.Next()
	return nil
}

func (bi *BadgerIterator) Next(value proto.Message) error {
	err := bi.currentValue(value)
	if err != nil {
		return err
	}

	bi.it.Next()
	return nil
}

func (bi *BadgerIterator) Close() error {
	bi.it.Close()
	return nil
}

func (bi *BadgerIterator) currentValue(value proto.Message) error {
	if !bi.it.Valid() {
		return ErrEndOfIterator
	}

	item := bi.it.Item() //important: this doesn't call Next()

	err := item.Value(func(val []byte) error {
		err := proto.Unmarshal(val, value)
		return err
	})

	if err != nil {
		return errors.Wrap(err, "couldn't unmarshal data")
	}

	return nil
}

func (bi *BadgerIterator) currentKey(key MonotonicallySerializable) error {
	if !bi.it.Valid() {
		return ErrEndOfIterator
	}

	item := bi.it.Item() //important: this doesn't call Next()

	byteKey := item.Key()
	strippedKey := byteKey[bi.prefixLength:] //TODO: maybe add function to do that

	err := key.MonotonicUnmarshal(strippedKey)
	return err
}

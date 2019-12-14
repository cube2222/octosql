package streaming

import (
	"io"

	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var ErrEndOfIterator = errors.New("end of iterator")

type Iterator interface {
	Next(proto.Message) error
	NextWithKey(key SortableSerialization, value proto.Message) error
	Rewind()
	io.Closer
}

type BadgerIterator struct {
	it *badger.Iterator
}

func NewBadgerIterator(it *badger.Iterator) *BadgerIterator {
	return &BadgerIterator{
		it: it,
	}
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

func (bi *BadgerIterator) currentKey(key SortableSerialization) error {
	if !bi.it.Valid() {
		return ErrEndOfIterator
	}

	item := bi.it.Item() //important: this doesn't call Next()

	byteKey := item.Key()

	err := key.SortedUnmarshal(byteKey)
	return err
}

func (bi *BadgerIterator) Next(value proto.Message) error {
	err := bi.currentValue(value)
	if err != nil {
		return err
	}

	bi.it.Next()
	return nil
}

func (bi *BadgerIterator) NextWithKey(key SortableSerialization, value proto.Message) error {
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

func (bi *BadgerIterator) Rewind() {
	bi.it.Rewind()
}

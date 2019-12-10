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

func (bi *BadgerIterator) Next(value proto.Message) error {
	if !bi.it.Valid() {
		return ErrEndOfIterator
	}

	item := bi.it.Item()
	defer bi.it.Next()

	err := item.Value(func(val []byte) error {
		err := proto.Unmarshal(val, value)
		return err
	})

	if err != nil {
		return errors.Wrap(err, "couldn't unmarshal data")
	}

	return nil
}

func (bi *BadgerIterator) Rewind() {
	bi.it.Rewind()
}

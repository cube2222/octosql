package storage

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type Map struct {
	tx StateTransaction
}

type MapIterator struct {
	it Iterator
}

func NewMap(tx StateTransaction) *Map {
	return &Map{
		tx: tx,
	}
}

func NewMapIterator(it Iterator) *MapIterator {
	return &MapIterator{
		it: it,
	}
}

func (hm *Map) Set(key MonotonicallySerializable, value proto.Message) error {
	byteKey := key.MonotonicMarshal()

	byteValue, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal value")
	}

	err = hm.tx.Set(byteKey, byteValue)
	if err != nil {
		return errors.Wrap(err, "couldn't add element to dictionary")
	}

	return nil
}

func (hm *Map) Get(key MonotonicallySerializable, value proto.Message) error {
	byteKey := key.MonotonicMarshal()

	data, err := hm.tx.Get(byteKey) //remove prefix from data
	if err != nil {
		return ErrKeyNotFound
	}

	err = proto.Unmarshal(data, value)
	return err
}

func (hm *Map) GetIteratorWithPrefix(prefix []byte) *MapIterator {
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix

	it := hm.tx.Iterator(options)

	return NewMapIterator(it)
}

func (hm *Map) GetIterator() *MapIterator {
	options := badger.DefaultIteratorOptions
	it := hm.tx.Iterator(options)
	it.Rewind()

	return NewMapIterator(it)
}

func (mi *MapIterator) Next(key MonotonicallySerializable, value proto.Message) error {
	err := mi.it.NextWithKey(key, value)
	return err
}

func (mi *MapIterator) Rewind() {
	mi.it.Rewind()
}

func (mi *MapIterator) Close() error {
	return mi.it.Close()
}

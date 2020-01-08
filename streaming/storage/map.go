package storage

import (
	"github.com/cube2222/octosql"
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

//Inserts the mapping key -> value to the map. If key was already present in the map
//then the value is overwritten.
func (hm *Map) Set(key MonotonicallySerializable, value proto.Message) error {
	byteKey := key.MonotonicMarshal()

	byteValue, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal value")
	}

	err = hm.tx.Set(byteKey, byteValue)
	if err != nil {
		return errors.Wrap(err, "couldn't add element to map")
	}

	return nil
}

//Returns the value corresponding to key. Returns ErrKeyNotFound if the given key
//doesn't correspond to a value in the map.
func (hm *Map) Get(key MonotonicallySerializable, value proto.Message) error {
	byteKey := key.MonotonicMarshal()

	data, err := hm.tx.Get(byteKey) //remove prefix from data
	if err == badger.ErrKeyNotFound {
		return ErrKeyNotFound
	} else if err != nil {
		return errors.Wrap(err, "something went wrong during the key look-up")
	}

	err = proto.Unmarshal(data, value)
	return err
}

//Returns an iterator with a specified prefix, that allows to iterate
//over a specified range of keys
func (hm *Map) GetIteratorWithPrefix(prefix []byte) *MapIterator {
	it := hm.tx.WithPrefix(prefix).Iterator(WithDefault())

	return NewMapIterator(it)
}

func (hm *Map) GetIterator() *MapIterator {
	it := hm.tx.Iterator(WithDefault())
	it.Rewind()

	return NewMapIterator(it)
}

//Removes a key from the map even if it wasn't present in it to begin with.
func (hm *Map) Delete(key MonotonicallySerializable) error {
	bytes := key.MonotonicMarshal()

	err := hm.tx.Delete(bytes)
	if err != nil { //if errors.Wrap(nil, ...) returns nil should this be just errors.Wrap(err, ...)
		return errors.Wrap(err, "couldn't delete key from badger storage")
	}

	return nil
}

//Clears the contents of the map.
// Important: To call map.Clear() one must close any iterators opened on that map
func (hm *Map) Clear() error {
	it := hm.GetIterator()
	defer it.Close()

	var key octosql.Value
	var value octosql.Value

	err := it.Next(&key, &value)

	for err != ErrEndOfIterator {
		if err != nil {
			return errors.Wrap(err, "failed to get next element from map")
		}

		bytes := key.MonotonicMarshal()

		err2 := hm.tx.Delete(bytes)
		if err2 != nil {
			return errors.Wrap(err, "couldn't remove value from map")
		}

		err = it.Next(&key, &value)
	}

	return nil
}

func (mi *MapIterator) Next(key MonotonicallySerializable, value proto.Message) error {
	return mi.it.NextWithKey(key, value)
}

func (mi *MapIterator) Rewind() {
	mi.it.Rewind()
}

func (mi *MapIterator) Close() error {
	return mi.it.Close()
}

package storage

import (
	"bytes"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/pkg/errors"
)

var maps = sync.Map{}

type sortedMap struct {
	sync.Mutex
	tree *btree.BTree
}

type keyValue struct {
	key   []byte
	value []byte
}

func (k *keyValue) Less(than btree.Item) bool {
	other, ok := than.(*keyValue)
	if !ok {
		return true
	}

	return bytes.Compare(k.key, other.key) == -1
}

type Map struct {
	internal *sortedMap
}

func NewMap(tx StateTransaction) *Map {
	return NewMapFromPrefix(tx.Prefix())
}

func NewMapFromPrefix(prefix string) *Map {
	tree := btree.New(2)

	newSortedMap := &sortedMap{
		Mutex: sync.Mutex{},
		tree:  tree,
	}

	actualMap, _ := maps.LoadOrStore(prefix, newSortedMap)

	return &Map{
		internal: actualMap.(*sortedMap),
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

	hm.internal.Lock()
	defer hm.internal.Unlock()

	hm.internal.tree.ReplaceOrInsert(&keyValue{
		key:   byteKey,
		value: byteValue,
	})

	return nil
}

//Returns the value corresponding to key. Returns ErrNotFound if the given key
//doesn't correspond to a value in the map.
func (hm *Map) Get(key MonotonicallySerializable, value proto.Message) error {
	byteKey := key.MonotonicMarshal()

	hm.internal.Lock()
	defer hm.internal.Unlock()

	out := hm.internal.tree.Get(&keyValue{
		key: byteKey,
	})
	if out == nil {
		return ErrNotFound
	}

	return proto.Unmarshal(out.(*keyValue).value, value)
}

// Returns an iterator with a specified prefix, that allows to iterate
// over a specified range of keys
// func (hm *Map) GetIteratorWithPrefix(prefix []byte) *MapIterator {
// 	it := hm.tx.WithPrefix(prefix).Iterator(WithDefault())
//
// 	return NewMapIterator(it)
// }

func (hm *Map) GetIterator(opts ...IteratorOption) *MapIterator {
	// TODO: Handle reverse
	hm.internal.Lock()
	defer hm.internal.Unlock()

	var items []*keyValue
	hm.internal.tree.Ascend(func(item btree.Item) bool {
		items = append(items, item.(*keyValue))
		return true
	})
	return NewMapIterator(items)
}

//Removes a key from the map even if it wasn't present in it to begin with.
func (hm *Map) Delete(key MonotonicallySerializable) error {
	byteKey := key.MonotonicMarshal()

	hm.internal.Lock()
	defer hm.internal.Unlock()

	hm.internal.tree.Delete(&keyValue{
		key: byteKey,
	})

	return nil
}

// Clears the contents of the map.
// Important: To call map.Clear() one must close any iterators opened on that map
func (hm *Map) Clear() error {
	hm.internal.tree = btree.New(2)
	return nil
}

type MapIterator struct {
	items []*keyValue
	i     int
}

func NewMapIterator(items []*keyValue) *MapIterator {
	return &MapIterator{
		items: items,
	}
}

func (mi *MapIterator) Next(key MonotonicallySerializable, value proto.Message) error {
	if mi.i == len(mi.items) {
		return ErrEndOfIterator
	}

	curItem := mi.items[mi.i]
	mi.i++

	if err := key.MonotonicUnmarshal(curItem.key); err != nil {
		return err
	}

	if err := proto.Unmarshal(curItem.value, value); err != nil {
		return err
	}
	return nil
}

func (mi *MapIterator) Close() error {
	return nil
}

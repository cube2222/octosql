package streaming

import (
	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

/*
	We want the keys used in badger to be sorted.
*/
type SortableSerialization interface {
	SortedMarshal() []byte
	SortedUnmarshal([]byte) error
}

var ErrKeyNotFound = errors.New("couldn't find key")

/* LinkedList */
type LinkedList struct {
	tx           StateTransaction
	elementCount int
	firstElement int
}

type LinkedListIterator struct {
	it Iterator
}

func NewLinkedList(tx StateTransaction) *LinkedList {
	return &LinkedList{
		tx:           tx,
		elementCount: 0,
		firstElement: 0,
	}
}

func NewLinkedListIterator(it Iterator) *LinkedListIterator {
	return &LinkedListIterator{
		it: it,
	}
}

func (ll *LinkedList) Append(value proto.Message) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	byteKey := octosql.SortedMarshalInt(ll.elementCount)

	err = ll.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to linked list")
	}

	ll.elementCount += 1
	return nil
}

func (ll *LinkedList) Peek(value proto.Message) error {
	firstKey := octosql.SortedMarshalInt(ll.firstElement)

	data, err := ll.tx.Get(firstKey)
	if err != nil {
		return errors.New("couldn't get the value of first element of list")
	}

	err = proto.Unmarshal(data, value)
	return err //TODO: wrap this?
}

//TODO: this is suboptimal since it calculates the firstKey twice, but meh...
func (ll *LinkedList) Pop(value proto.Message) error {
	err := ll.Peek(value)
	if err != nil {
		return err
	}

	firstKey := octosql.SortedMarshalInt(ll.firstElement)

	err = ll.tx.Delete(firstKey)
	if err != nil {
		return errors.New("couldn't delete first element of list")
	}

	ll.firstElement++
	return nil
}

func (ll *LinkedList) GetIterator() *LinkedListIterator {
	it := ll.tx.Iterator(badger.DefaultIteratorOptions)
	it.Rewind()

	return NewLinkedListIterator(it)
}

func (lli *LinkedListIterator) Next(value proto.Message) error {
	err := lli.it.Next(value)
	if err != nil {
		return errors.Wrap(err, "couldn't get next element from linked list")
	}

	return nil
}

func (lli *LinkedListIterator) Close() error {
	return lli.it.Close()
}

func (lli *LinkedListIterator) Rewind() {
	lli.it.Rewind()
}

/* Map */
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

func (hm *Map) Set(key SortableSerialization, value proto.Message) error {
	byteKey := key.SortedMarshal()

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

func (hm *Map) Get(key SortableSerialization, value proto.Message) error {
	byteKey := key.SortedMarshal()

	data, err := hm.tx.Get(byteKey)
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
	return NewMapIterator(it)
}

func (mi *MapIterator) Next(key SortableSerialization, value proto.Message) error {
	err := mi.it.NextWithKey(key, value)
	return err
}

func (mi *MapIterator) Rewind() {
	mi.it.Rewind()
}

func (mi *MapIterator) Close() error {
	return mi.it.Close()
}

/* ValueState */
type ValueState struct {
	tx StateTransaction
}

func NewValueState(tx StateTransaction) *ValueState {
	return &ValueState{
		tx: tx,
	}
}

func (vs *ValueState) Set(value proto.Message) error {
	byteValue, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal value")
	}

	err = vs.tx.Set(nil, byteValue)
	if err != nil {
		return errors.Wrap(err, "couldn't set value")
	}

	return nil
}

func (vs *ValueState) Get(value proto.Message) error {
	data, err := vs.tx.Get(nil)
	if err != nil {
		return ErrKeyNotFound
	}

	err = proto.Unmarshal(data, value)
	return err
}

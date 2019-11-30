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
	io.Closer
}

/* LinkedList */
type LinkedList struct {
	tx           *badgerTransaction
	elementCount int
}

func NewLinkedList(tx *badgerTransaction) *LinkedList {
	return &LinkedList{
		tx:           tx,
		elementCount: 0,
	}
}

func (ll *LinkedList) Append(value proto.Message) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	err = ll.tx.Set(intToByteSlice(ll.elementCount), data)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to linked list")
	}

	ll.elementCount += 1
	return nil
}

func (ll *LinkedList) GetAll() (*LinkedListIterator, error) {
	data := make([][]byte, ll.elementCount)

	for i := 0; i < ll.elementCount; i++ {
		el, err := ll.tx.Get(intToByteSlice(i))
		if err != nil {
			return nil, errors.Wrap(err, "couldn't read data from linked list")
		}

		data[i] = el
	}

	return newLinkedListIterator(data), nil
}

type LinkedListIterator struct {
	data   [][]byte
	index  int
	closed bool
}

func newLinkedListIterator(data [][]byte) *LinkedListIterator {
	return &LinkedListIterator{
		data:   data,
		index:  0,
		closed: false,
	}
}

func (lli *LinkedListIterator) Next(value proto.Message) error {
	if lli.closed {
		return ErrEndOfIterator
	}

	err := proto.Unmarshal(lli.data[lli.index], value)
	if err != nil {
		return errors.Wrap(err, "couldn't unmarshal data")
	}

	lli.index += 1
	if lli.index == len(lli.data) {
		lli.Close()
	}

	return nil
}

func (lli *LinkedListIterator) Close() {
	lli.closed = true
}

func intToByteSlice(x int) []byte {
	return []byte(string(x))
}

/* Map */
type Map struct {
	tx *badgerTransaction
}

func NewMap(tx *badgerTransaction) *Map {
	return &Map{
		tx: tx,
	}
}

func (hm *Map) Set(key []byte, value proto.Message) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal value")
	}

	err = hm.tx.Set(key, data)
	if err != nil {
		return errors.Wrap(err, "couldn't add element to dictionary")
	}

	return nil
}

func (hm *Map) Get(key []byte, value proto.Message) error {
	data, err := hm.tx.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get element from dictionary")
	}

	err = proto.Unmarshal(data, value)
	return err
}

func (hm *Map) GetAllWithPrefix(prefix []byte) *MapIterator {
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix

	it := hm.tx.tx.NewIterator(options)

	return newMapIterator(it)
}

func (hm *Map) GetAll() *MapIterator {
	options := badger.DefaultIteratorOptions

	it := hm.tx.tx.NewIterator(options)

	return newMapIterator(it)
}

type MapIterator struct {
	it *badger.Iterator
}

func newMapIterator(it *badger.Iterator) *MapIterator {
	return &MapIterator{
		it: it,
	}
}

func (mi *MapIterator) Next(message proto.Message) error {
	if !mi.it.Valid() {
		return ErrEndOfIterator
	}

	mi.it.Next()
	item := mi.it.Item()

	err := item.Value(func(val []byte) error {
		err := proto.Unmarshal(val, message)
		return err
	})

	if err != nil {
		return errors.Wrap(err, "couldn't unmarshal data")
	}

	return nil
}

func (mi *MapIterator) Close() {
	mi.it.Close()
}

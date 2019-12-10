package streaming

import (
	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

/* LinkedList */
type LinkedList struct {
	tx           StateTransaction
	elementCount int
}

func NewLinkedList(tx StateTransaction) *LinkedList {
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

	key := octosql.MakeInt(ll.elementCount)
	byteKey, err := proto.Marshal(&key)
	if err != nil {
		return errors.Wrap(err, "couldn't translate key to bytes")
	}

	err = ll.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to linked list")
	}

	ll.elementCount += 1
	return nil
}

func (ll *LinkedList) GetAll() Iterator {
	it := ll.tx.Iterator(badger.DefaultIteratorOptions)
	it.Rewind()

	return it
}

/* Map */
type Map struct {
	tx StateTransaction
}

func NewMap(tx StateTransaction) *Map {
	return &Map{
		tx: tx,
	}
}

func (hm *Map) Set(key, value proto.Message) error {
	byteKey, err := proto.Marshal(key)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal key")
	}

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

func (hm *Map) Get(key, value proto.Message) error {
	byteKey, err := proto.Marshal(key)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal key")
	}

	data, err := hm.tx.Get(byteKey)
	if err != nil {
		return errors.Wrap(err, "couldn't get element from dictionary")
	}

	err = proto.Unmarshal(data, value)
	return err
}

func (hm *Map) GetAllWithPrefix(prefix []byte) Iterator {
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix

	it := hm.tx.Iterator(options)

	return it
}

func (hm *Map) GetAll() Iterator {
	options := badger.DefaultIteratorOptions
	it := hm.tx.Iterator(options)
	return it
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
		return errors.Wrap(err, "couldn't get element from dictionary")
	}

	err = proto.Unmarshal(data, value)
	return err
}

package streaming

import (
	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

//TODO: these values might be reduced to "l", "f", and "v" to shorten the keys
var (
	linkedListLengthKey       = []byte("length")
	linkedListFirstElementKey = []byte("first")
	linkedListValueKeyPrefix  = []byte("value")
)

var ErrKeyNotFound = errors.New("couldn't find key")

/*
	A type that implements this interface must have a MonotonicMarshal method,
	that has the property, that if x <= y (where <= is the less-equal relation
	defined on a certain type, for example lexicographical order on strings) then
	MonotonicMarshal(x) <= MonotonicMarshal(y) (where <= is the lexicographical order on
	[]byte). Since in mathematics such a function is called Monotonous, thus the name.
	A MonotonicUnmarshal which is the inverse of MonotonicMarshal is also required.

	WARNING: Float and Object are Serializable, but the serialization is not Monotonic!
*/
type MonotonicallySerializable interface {
	MonotonicMarshal() []byte
	MonotonicUnmarshal([]byte) error
}

/* LinkedList */
type LinkedList struct {
	tx           StateTransaction
	initialized  bool
	elementCount int
	firstElement int
}

type LinkedListIterator struct {
	it Iterator
}

func NewLinkedList(tx StateTransaction) *LinkedList {
	return &LinkedList{
		tx:           tx,
		initialized:  false,
		elementCount: 0,
		firstElement: 0,
	}
}

func (ll *LinkedList) initialize() error {
	length, err := ll.getLength()
	if err != nil {
		return errors.Wrap(err, "couldn't initialize linked list")
	}

	first, err := ll.getFirst()
	if err != nil {
		return errors.Wrap(err, "couldn't initialize linked list")
	}

	ll.elementCount = length
	ll.firstElement = first
	ll.initialized = true

	return nil
}

func (ll *LinkedList) getLength() (int, error) {
	return ll.getAttribute(linkedListLengthKey)
}

func (ll *LinkedList) getFirst() (int, error) {
	return ll.getAttribute(linkedListFirstElementKey)
}

func (ll *LinkedList) getAttribute(attr []byte) (int, error) {
	value, err := ll.tx.Get(attr)
	switch err {
	case badger.ErrKeyNotFound:
		err2 := ll.tx.Set(attr, octosql.MonotonicMarshalInt(0))
		if err2 != nil {
			return 0, errors.Wrapf(err2, "couldn't initialize linked list %s field", string(attr))
		}

		return 0, nil
	case nil:
		return octosql.MonotonicUnmarshalInt(value)
	default:
		return 0, errors.Wrapf(err, "couldn't read %s of linked list", string(attr))
	}
}

func (ll *LinkedList) incLength() error {
	return ll.incAttribute(linkedListLengthKey)
}

func (ll *LinkedList) incFirst() error {
	return ll.incAttribute(linkedListFirstElementKey)
}

func (ll *LinkedList) incAttribute(attr []byte) error {
	value, err := ll.getAttribute(attr)
	if err != nil {
		return err
	}

	newValue := octosql.MonotonicMarshalInt(value + 1)

	err = ll.tx.Set(attr, newValue)
	if err != nil {
		return errors.Wrap(err, "failed to set new value")
	}

	return nil
}

func NewLinkedListIterator(it Iterator) *LinkedListIterator {
	return &LinkedListIterator{
		it: it,
	}
}

func (ll *LinkedList) Append(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize linked list in append")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	return ll.appendBytes(data)
}

func (ll *LinkedList) appendBytes(data []byte) error {
	byteKey := getIndexKey(ll.elementCount)

	err := ll.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to linked list")
	}

	err = ll.incLength()
	if err != nil {
		return errors.Wrap(err, "failed to increase linked list length attribute")
	}

	ll.elementCount += 1
	return nil
}

func (ll *LinkedList) Peek(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize linked list in append")
		}
	}

	firstKey := getIndexKey(ll.firstElement)

	data, err := ll.tx.Get(firstKey)
	if err != nil {
		return errors.New("couldn't get the value of first element of list")
	}

	err = proto.Unmarshal(data, value)
	return errors.Wrap(err, "couldn't unmarshal the first element")
}

func (ll *LinkedList) Pop(value proto.Message) error {
	//there is no need to call initialize here, since Peek calls initialize

	err := ll.Peek(value)
	if err != nil {
		return err
	}

	firstKey := getIndexKey(ll.firstElement)

	err = ll.tx.Delete(firstKey)
	if err != nil {
		return errors.New("couldn't delete first element of list")
	}

	err = ll.incFirst()
	if err != nil {
		return errors.Wrap(err, "failed to increase linked list first attribute")
	}

	ll.firstElement++
	return nil
}

func (ll *LinkedList) GetIterator() *LinkedListIterator {
	it := ll.tx.WithPrefix(linkedListValueKeyPrefix).Iterator(badger.DefaultIteratorOptions)
	it.Rewind()

	return NewLinkedListIterator(it)
}

func getIndexKey(index int) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, linkedListValueKeyPrefix...)
	bytes = append(bytes, octosql.MonotonicMarshalInt(index)...)

	return bytes
}

func (lli *LinkedListIterator) Next(value proto.Message) error {
	err := lli.it.Next(value)
	if err == ErrEndOfIterator || err == nil {
		return err
	}

	return errors.Wrap(err, "couldn't read next element")
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

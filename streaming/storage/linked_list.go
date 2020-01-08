package storage

import (
	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var ErrEmptyList = errors.New("list is empty")

var (
	linkedListLengthKey       = []byte("length")
	linkedListFirstElementKey = []byte("first")
	linkedListValueKeyPrefix  = []byte("value")
)

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

func NewLinkedListIterator(it Iterator) *LinkedListIterator {
	return &LinkedListIterator{
		it: it,
	}
}

//Adds new element to the list
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

//Returns the first element of the list without removing it
//Returns ErrEmptyList if the list is empty
func (ll *LinkedList) Peek(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize linked list in append")
		}
	}

	if ll.firstElement >= ll.elementCount {
		return ErrEmptyList
	}

	firstKey := getIndexKey(ll.firstElement)

	data, err := ll.tx.Get(firstKey)

	if err != nil {
		return errors.New("couldn't get the value of first element of list")
	}

	err = proto.Unmarshal(data, value)
	return errors.Wrap(err, "couldn't unmarshal the first element")
}

//Removes the first element from the list and returns it
//Returns ErrEmptyList if the list is empty
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

//Clears all contents of the linked list including the metadata
func (ll *LinkedList) Clear() error {
	for ll.firstElement < ll.elementCount {
		key := getIndexKey(ll.firstElement)
		err := ll.tx.Delete(key)

		if err != nil {
			return errors.Wrap(err, "couldn't clear the list")
		}

		err = ll.incFirst()
		if err != nil {
			return errors.Wrap(err, "couldn't increase first element index")
		}

		ll.firstElement++
	}

	err := ll.tx.Delete(linkedListFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "couldn't clear first element index metadata")
	}

	err = ll.tx.Delete(linkedListLengthKey)
	if err != nil {
		return errors.Wrap(err, "couldn't clear length of linked list metadata")
	}

	/* Generally if someone uses Clear() on a linkedList, then they shouldn't
	use the same one to add new values. In case they do we can set these to 0.
	*/
	ll.firstElement = 0
	ll.elementCount = 0

	return nil
}

func (ll *LinkedList) GetIterator() *LinkedListIterator {
	it := ll.tx.WithPrefix(linkedListValueKeyPrefix).Iterator(WithDefault())
	it.Rewind()

	return NewLinkedListIterator(it)
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

func getIndexKey(index int) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, linkedListValueKeyPrefix...)
	bytes = append(bytes, octosql.MonotonicMarshalInt(index)...)

	return bytes
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

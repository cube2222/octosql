package storage

import (
	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var ErrEmptyQueue = errors.New("the queue is empty")

var (
	dequeueLastElementKey  = []byte("last")
	dequeueFirstElementKey = []byte("first")
	dequeueValueKeyPrefix  = []byte("value")
)

type Dequeue struct {
	tx           StateTransaction
	initialized  bool
	lastElement  int
	firstElement int
}

type DequeueIterator struct {
	it Iterator
}

func NewDequeue(tx StateTransaction) *Dequeue {
	return &Dequeue{
		tx:           tx,
		initialized:  false,
		lastElement:  0,
		firstElement: 0,
	}
}

func NewDequeueIterator(it Iterator) *DequeueIterator {
	return &DequeueIterator{
		it: it,
	}
}

//Adds new element to the front of the queue
func (ll *Dequeue) PushFront(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PushFront")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	return ll.appendBytes(data, ll.firstElement, ll.decFirst)
}

//Adds new element to the end of the queue
func (ll *Dequeue) PushBack(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PushBack()")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	return ll.appendBytes(data, ll.lastElement, ll.incLast)
}

func (ll *Dequeue) appendBytes(data []byte, index int, modifier func() error) error {
	byteKey := getIndexKey(index)

	err := ll.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to the queue")
	}

	err = modifier()
	if err != nil {
		return errors.Wrap(err, "failed to actualize queue attributes")
	}

	return nil
}

//Removes the first element from the queue and returns it
//Returns ErrEmptyQueue if the list is empty
func (ll *Dequeue) PopFront(value proto.Message) error {
	//there is no need to call initialize here, since PeekFront calls initialize
	err := ll.PeekFront(value)
	if err != nil {
		return err
	}

	return ll.pop(value, ll.firstElement, ll.incFirst)
}

//Removes the last element from the queue and returns it
//Returns ErrEmptyQueue if the list is empty
func (ll *Dequeue) PopBack(value proto.Message) error {
	//there is no need to call initialize here, since PeekFront calls initialize
	err := ll.PeekBack(value)
	if err != nil {
		return err
	}

	return ll.pop(value, ll.lastElement, ll.decLast)
}

func (ll *Dequeue) pop(value proto.Message, index int, modifier func() error) error {
	err := ll.peekIndex(value, index)
	if err != nil {
		return err
	}

	key := getIndexKey(index)

	err = ll.tx.Delete(key)
	if err != nil {
		return errors.New("failed to delete element from queue in pop")
	}

	err = modifier()
	return errors.Wrap(err, "failed to actualize queue attributes in pop")
}

//Returns the first element of the queue without removing it
//Returns ErrEmptyQueue if the queue is empty
func (ll *Dequeue) PeekFront(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PeekFront()")
		}
	}

	return ll.peekIndex(value, ll.firstElement)
}

//Returns the last element of the queue without removing it
//Returns ErrEmptyQueue if the queue is empty
func (ll *Dequeue) PeekBack(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PeekBack()")
		}
	}

	return ll.peekIndex(value, ll.lastElement)
}

//we don't check if the queue has been initialized here, since we only
//call peekIndex from PeekFront/PeekBack, which both initialize the queue
func (ll *Dequeue) peekIndex(value proto.Message, index int) error {
	if ll.firstElement >= ll.lastElement {
		return ErrEmptyQueue
	}

	key := getIndexKey(index)

	data, err := ll.tx.Get(key)
	if err != nil {
		return errors.New("couldn't get value of element in peek")
	}

	err = proto.Unmarshal(data, value)
	return errors.Wrap(err, "couldn't unmarshal the element in peek")
}

//Clears all contents of the queue including the metadata
func (ll *Dequeue) Clear() error {
	for ll.firstElement < ll.lastElement {
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

	err := ll.tx.Delete(dequeueFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "couldn't clear first element index metadata")
	}

	err = ll.tx.Delete(dequeueLastElementKey)
	if err != nil {
		return errors.Wrap(err, "couldn't clear last element index metadata")
	}

	/* Generally if someone uses Clear() on the a queue (as in a namespace), then they shouldn't
	use the same one to add new values. In case they do we can set these to 0.
	*/
	ll.firstElement = 0
	ll.lastElement = 0

	return nil
}

func (ll *Dequeue) GetIterator() *DequeueIterator {
	it := ll.tx.WithPrefix(dequeueValueKeyPrefix).Iterator(WithDefault())
	return NewDequeueIterator(it)
}

func (lli *DequeueIterator) Next(value proto.Message) error {
	err := lli.it.Next(value)
	if err == ErrEndOfIterator || err == nil {
		return err
	}

	return errors.Wrap(err, "couldn't read next element")
}

func (lli *DequeueIterator) Close() error {
	return lli.it.Close()
}

func getIndexKey(index int) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, dequeueValueKeyPrefix...)
	bytes = append(bytes, octosql.MonotonicMarshalInt64(int64(index))...)

	return bytes
}

func (ll *Dequeue) initialize() error {
	last, err := ll.getLast()
	if err != nil {
		return errors.Wrap(err, "couldn't initialize the queue last attribute")
	}

	first, err := ll.getFirst()
	if err != nil {
		return errors.Wrap(err, "couldn't initialize the queue first attribute")
	}

	ll.lastElement = last
	ll.firstElement = first
	ll.initialized = true

	return nil
}

func (ll *Dequeue) getLast() (int, error) {
	return ll.getAttribute(dequeueLastElementKey)
}

func (ll *Dequeue) getFirst() (int, error) {
	return ll.getAttribute(dequeueFirstElementKey)
}

func (ll *Dequeue) getAttribute(attr []byte) (int, error) {
	value, err := ll.tx.Get(attr)
	switch err {
	case badger.ErrKeyNotFound:
		err2 := ll.tx.Set(attr, octosql.MonotonicMarshalInt64(0))
		if err2 != nil {
			return 0, errors.Wrapf(err2, "couldn't initialize queue %s field", string(attr))
		}

		return 0, nil
	case nil:
		i, err := octosql.MonotonicUnmarshalInt64(value)
		return int(i), err
	default:
		return 0, errors.Wrapf(err, "couldn't read %s of queue", string(attr))
	}
}

func (ll *Dequeue) incLast() error {
	err := ll.incAttribute(dequeueLastElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to increase queue last attribute")
	}

	ll.lastElement += 1
	return nil
}

func (ll *Dequeue) incFirst() error {
	err := ll.incAttribute(dequeueFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to increase queue first attribute")
	}

	ll.firstElement += 1
	return nil
}

func (ll *Dequeue) decLast() error {
	err := ll.decAttribute(dequeueLastElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to decrease queue last attribute")
	}

	ll.lastElement -= 1
	return nil
}

func (ll *Dequeue) decFirst() error {
	err := ll.decAttribute(dequeueFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to decrease queue first attribute")
	}

	ll.firstElement -= 1
	return nil
}

func (ll *Dequeue) incAttribute(attr []byte) error {
	return ll.modifyAttribute(attr, func(v int64) int64 {
		return v + 1
	})
}

func (ll *Dequeue) decAttribute(attr []byte) error {
	return ll.modifyAttribute(attr, func(v int64) int64 {
		return v - 1
	})
}

func (ll *Dequeue) modifyAttribute(attr []byte, modifier func(int64) int64) error {
	value, err := ll.getAttribute(attr)
	if err != nil {
		return err
	}

	newValue := octosql.MonotonicMarshalInt64(modifier(int64(value)))

	err = ll.tx.Set(attr, newValue)
	if err != nil {
		return errors.Wrap(err, "failed to set new value")
	}

	return nil
}

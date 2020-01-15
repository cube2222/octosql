package storage

import (
	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var ErrEmptyQueue = errors.New("the queue is empty")

var (
	dequeLastElementKey  = []byte("last")
	dequeFirstElementKey = []byte("first")
	dequeValueKeyPrefix  = []byte("value")
	defaultValues        = map[string]int{
		string(dequeFirstElementKey): 0,
		string(dequeLastElementKey):  1,
	}
)

type Deque struct {
	tx           StateTransaction
	initialized  bool
	lastElement  int
	firstElement int
}

type DequeIterator struct {
	it Iterator
}

func NewDeque(tx StateTransaction) *Deque {
	return &Deque{
		tx:           tx,
		initialized:  false,
		lastElement:  1,
		firstElement: 0,
	}
}

func NewDequeIterator(it Iterator) *DequeIterator {
	return &DequeIterator{
		it: it,
	}
}

//Adds new element to the front of the queue
func (ll *Deque) PushFront(value proto.Message) error {
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
func (ll *Deque) PushBack(value proto.Message) error {
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

func (ll *Deque) appendBytes(data []byte, index int, modifier func() error) error {
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
func (ll *Deque) PopFront(value proto.Message) error {
	//there is no need to call initialize here, since PeekFront calls initialize
	err := ll.PeekFront(value)
	if err != nil {
		return err
	}

	return ll.pop(value, ll.firstElement+1, ll.incFirst)
}

//Removes the last element from the queue and returns it
//Returns ErrEmptyQueue if the list is empty
func (ll *Deque) PopBack(value proto.Message) error {
	//there is no need to call initialize here, since PeekFront calls initialize
	err := ll.PeekBack(value)
	if err != nil {
		return err
	}

	return ll.pop(value, ll.lastElement-1, ll.decLast)
}

func (ll *Deque) pop(value proto.Message, index int, modifier func() error) error {
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
func (ll *Deque) PeekFront(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PeekFront()")
		}
	}

	return ll.peekIndex(value, ll.firstElement+1)
}

//Returns the last element of the queue without removing it
//Returns ErrEmptyQueue if the queue is empty
func (ll *Deque) PeekBack(value proto.Message) error {
	if !ll.initialized {
		err := ll.initialize()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PeekBack()")
		}
	}

	return ll.peekIndex(value, ll.lastElement-1)
}

//we don't check if the queue has been initialized here, since we only
//call peekIndex from PeekFront/PeekBack, which both initialize the queue
func (ll *Deque) peekIndex(value proto.Message, index int) error {
	if ll.lastElement-ll.firstElement <= 1 {
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

func (ll *Deque) Print() error { //add initialize here (?)
	var value octosql.Value
	it := ll.tx.WithPrefix(dequeValueKeyPrefix).Iterator()

	defer func() {
		_ = it.Close() //TODO: panic here?
	}()

	for {
		err := it.Next(&value)
		if err == ErrEndOfIterator {
			break
		} else if err != nil {
			return errors.Wrap(err, "error in iterator next")
		}

		println(value.Show())
	}

	return nil
}

//Clears all contents of the queue including the metadata
func (ll *Deque) Clear() error {
	for ll.firstElement < ll.lastElement-1 {
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

	err := ll.tx.Delete(dequeFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "couldn't clear first element index metadata")
	}

	err = ll.tx.Delete(dequeLastElementKey)
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

func (ll *Deque) GetIterator() *DequeIterator {
	it := ll.tx.WithPrefix(dequeValueKeyPrefix).Iterator(WithDefault())
	return NewDequeIterator(it)
}

func (lli *DequeIterator) Next(value proto.Message) error {
	err := lli.it.Next(value)
	if err == ErrEndOfIterator || err == nil {
		return err
	}

	return errors.Wrap(err, "couldn't read next element")
}

func (lli *DequeIterator) Close() error {
	return lli.it.Close()
}

func getIndexKey(index int) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, dequeValueKeyPrefix...)
	bytes = append(bytes, octosql.MonotonicMarshalInt64(int64(index))...)

	return bytes
}

func (ll *Deque) initialize() error {
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

func (ll *Deque) getLast() (int, error) {
	return ll.getAttribute(dequeLastElementKey)
}

func (ll *Deque) getFirst() (int, error) {
	return ll.getAttribute(dequeFirstElementKey)
}

func (ll *Deque) getAttribute(attr []byte) (int, error) {
	value, err := ll.tx.Get(attr)
	stringAttribute := string(attr)
	switch err {
	case badger.ErrKeyNotFound:
		value, ok := defaultValues[stringAttribute]
		if !ok {
			return 0, errors.New("this byte slice isn't a queue attribute")
		}

		err2 := ll.tx.Set(attr, octosql.MonotonicMarshalInt64(int64(value)))
		if err2 != nil {
			return 0, errors.Wrapf(err2, "couldn't initialize queue %s field", stringAttribute)
		}

		return value, nil
	case nil:
		i, err := octosql.MonotonicUnmarshalInt64(value)
		return int(i), err
	default:
		return 0, errors.Wrapf(err, "couldn't read %s of queue", stringAttribute)
	}
}

func (ll *Deque) incLast() error {
	err := ll.incAttribute(dequeLastElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to increase queue last attribute")
	}

	ll.lastElement += 1
	return nil
}

func (ll *Deque) incFirst() error {
	err := ll.incAttribute(dequeFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to increase queue first attribute")
	}

	ll.firstElement += 1
	return nil
}

func (ll *Deque) decLast() error {
	err := ll.decAttribute(dequeLastElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to decrease queue last attribute")
	}

	ll.lastElement -= 1
	return nil
}

func (ll *Deque) decFirst() error {
	err := ll.decAttribute(dequeFirstElementKey)
	if err != nil {
		return errors.Wrap(err, "failed to decrease queue first attribute")
	}

	ll.firstElement -= 1
	return nil
}

func (ll *Deque) incAttribute(attr []byte) error {
	return ll.modifyAttribute(attr, func(v int64) int64 {
		return v + 1
	})
}

func (ll *Deque) decAttribute(attr []byte) error {
	return ll.modifyAttribute(attr, func(v int64) int64 {
		return v - 1
	})
}

func (ll *Deque) modifyAttribute(attr []byte, modifier func(int64) int64) error {
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

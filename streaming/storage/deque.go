package storage

import (
	"github.com/cube2222/octosql"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

/*
	Name clarification:
	In a deque 1, 2, 3, 4 the Front is the 1 and the Back is the 4. This way
	a Push/PopFront modify the front attributes, and Push/PopBack modify the back attributes.
*/
const (
	defaultFirstFreeFrontSpot = 0
	defaultFirstFreeBackSpot  = 1
)

var (
	dequeValueKeyPrefix     = []byte("value")
	dequeFirstFreeFrontSpot = []byte("first")
	dequeFirstFreeBackSpot  = []byte("last")
	defaultValues           = map[string]int{
		string(dequeFirstFreeFrontSpot): defaultFirstFreeFrontSpot,
		string(dequeFirstFreeBackSpot):  defaultFirstFreeBackSpot,
	}
)

type Deque struct {
	tx StateTransaction

	initializedFront bool
	initializedBack  bool

	firstFreeFrontSpot int
	firstFreeBackSpot  int
}

type DequeIterator struct {
	it Iterator
}

func NewDeque(tx StateTransaction) *Deque {
	return &Deque{
		tx:                 tx,
		initializedFront:   false,
		initializedBack:    false,
		firstFreeFrontSpot: defaultFirstFreeFrontSpot,
		firstFreeBackSpot:  defaultFirstFreeBackSpot,
	}
}

func NewDequeIterator(it Iterator) *DequeIterator {
	return &DequeIterator{
		it: it,
	}
}

//Adds new element to the front of the queue
func (dq *Deque) PushFront(value proto.Message) error {
	if !dq.initializedFront {
		err := dq.initializeFront()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PushFront")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	return dq.appendBytes(data, dq.firstFreeFrontSpot, dq.decreaseFirstFreeFrontSpot)
}

//Adds new element to the back of the queue
func (dq *Deque) PushBack(value proto.Message) error {
	if !dq.initializedBack {
		err := dq.initializeBack()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PushBack")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value")
	}

	return dq.appendBytes(data, dq.firstFreeBackSpot, dq.increaseFirstFreeBackSpot)
}

//This is an auxiliary function used in PushBack and PushFront. The modifier is needed,
//to move the firstFreeFrontSpot to the left (PushFront) or the firstFreeBackSpot to the
// right (PushBack). These functions call appendBytes with modifier set to
// dq.decreaseFirstFreeFrontSpot and dq.increaseFirstFreeBackSpot respectively.
func (dq *Deque) appendBytes(data []byte, index int, modifier func() error) error {
	byteKey := getIndexKey(index)

	err := dq.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to the queue")
	}

	err = modifier() //modifier is a function of the dq so it modifies dq
	if err != nil {
		return errors.Wrap(err, "failed to actualize queue attributes")
	}

	return nil
}

//Removes the first element from the queue and returns it
//Returns ErrNotFound if the list is empty
func (dq *Deque) PopFront(value proto.Message) error {
	//there is no need to call initialize here, since PeekFront calls initialize
	err := dq.PeekFront(value)
	if err != nil {
		return err
	}

	return dq.pop(value, dq.firstFreeFrontSpot+1, dq.increaseFirstFreeFrontSpot)
}

//Removes the last element from the queue and returns it
//Returns ErrNotFound if the list is empty
func (dq *Deque) PopBack(value proto.Message) error {
	//there is no need to call initialize here, since PeekBack calls initialize
	err := dq.PeekBack(value)
	if err != nil {
		return err
	}

	return dq.pop(value, dq.firstFreeBackSpot-1, dq.decreaseFirstFreeBackSpot)
}

func (dq *Deque) pop(value proto.Message, index int, modifier func() error) error {
	err := dq.peekIndex(value, index)
	if err == ErrNotFound {
		return ErrNotFound
	} else if err != nil {
		return errors.Wrap(err, "something went wrong during peek")
	}

	key := getIndexKey(index)

	err = dq.tx.Delete(key)
	if err != nil {
		return errors.Wrap(err, "failed to delete element from queue in pop")
	}

	err = modifier()
	if err != nil {
		return errors.Wrap(err, "failed to actualize queue attributes in pop")
	}

	return nil
}

//Returns the first element of the queue without removing it
//Returns ErrNotFound if the queue is empty
func (dq *Deque) PeekFront(value proto.Message) error {
	if !dq.initializedFront {
		err := dq.initializeFront()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PeekFront")
		}
	}

	return dq.peekIndex(value, dq.firstFreeFrontSpot+1)
}

//Returns the last element of the queue without removing it
//Returns ErrNotFound if the queue is empty
func (dq *Deque) PeekBack(value proto.Message) error {
	if !dq.initializedBack {
		err := dq.initializeBack()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in queue.PeekBack")
		}
	}

	return dq.peekIndex(value, dq.firstFreeBackSpot-1)
}

//we don't check if the queue has been initialized here, since we only
//call peekIndex from PeekFront/PeekBack, which both initialize the queue
func (dq *Deque) peekIndex(value proto.Message, index int) error {
	key := getIndexKey(index)

	data, err := dq.tx.Get(key)
	if err == ErrNotFound {
		return ErrNotFound
	} else if err != nil {
		return errors.Wrap(err, "an error occurred during Get")
	}

	err = proto.Unmarshal(data, value)
	if err != nil {
		return errors.Wrap(err, "couldn't unmarshal the element in peek")
	}

	return nil
}

//Clears all contents of the queue including the metadata
func (dq *Deque) Clear() error {
	for dq.firstFreeBackSpot-dq.firstFreeFrontSpot > 1 {
		key := getIndexKey(dq.firstFreeFrontSpot + 1)
		err := dq.tx.Delete(key)

		if err != nil {
			return errors.Wrap(err, "couldn't clear the list")
		}

		err = dq.increaseFirstFreeFrontSpot()
		if err != nil {
			return errors.Wrap(err, "couldn't increase first element index")
		}
	}

	err := dq.tx.Delete(dequeFirstFreeFrontSpot)
	if err != nil {
		return errors.Wrap(err, "couldn't clear first element index metadata")
	}

	err = dq.tx.Delete(dequeFirstFreeBackSpot)
	if err != nil {
		return errors.Wrap(err, "couldn't clear last element index metadata")
	}

	/* Generally if someone uses Clear() on the a queue (as in a namespace), then they shouldn't
	use the same one to add new values. In case they do we can set these to defaults.
	*/
	dq.firstFreeFrontSpot = 0
	dq.firstFreeBackSpot = 1
	dq.initializedFront = false

	return nil
}

func (dq *Deque) GetIterator() *DequeIterator {
	it := dq.tx.WithPrefix(dequeValueKeyPrefix).Iterator(WithDefault())
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

func (dq *Deque) initializeFront() error {
	front, err := dq.getFrontSpot()
	if err != nil {
		return errors.Wrap(err, "couldn't initialize the front attribute of queue")
	}

	dq.firstFreeFrontSpot = front
	dq.initializedFront = true

	return nil
}

func (dq *Deque) initializeBack() error {
	back, err := dq.getBackSpot()
	if err != nil {
		return errors.Wrap(err, "couldn't initialize the back attribute of queue")
	}

	dq.firstFreeBackSpot = back
	dq.initializedBack = true

	return nil
}

func (dq *Deque) getBackSpot() (int, error) {
	return dq.getAttribute(dequeFirstFreeBackSpot)
}

func (dq *Deque) getFrontSpot() (int, error) {
	return dq.getAttribute(dequeFirstFreeFrontSpot)
}

func (dq *Deque) increaseFirstFreeBackSpot() error {
	err := dq.increaseAttribute(dequeFirstFreeBackSpot)
	if err != nil {
		return errors.Wrap(err, "failed to increase queue's back spot attribute")
	}

	dq.firstFreeBackSpot += 1
	return nil
}

func (dq *Deque) increaseFirstFreeFrontSpot() error {
	err := dq.increaseAttribute(dequeFirstFreeFrontSpot)
	if err != nil {
		return errors.Wrap(err, "failed to increase queue's front spot attribute")
	}

	dq.firstFreeFrontSpot += 1
	return nil
}

func (dq *Deque) increaseAttribute(attr []byte) error {
	return dq.modifyAttribute(attr, func(v int64) int64 {
		return v + 1
	})
}

func (dq *Deque) decreaseFirstFreeBackSpot() error {
	err := dq.decreaseAttribute(dequeFirstFreeBackSpot)
	if err != nil {
		return errors.Wrap(err, "failed to decrease queue's back spot attribute")
	}

	dq.firstFreeBackSpot -= 1
	return nil
}

func (dq *Deque) decreaseFirstFreeFrontSpot() error {
	err := dq.decreaseAttribute(dequeFirstFreeFrontSpot)
	if err != nil {
		return errors.Wrap(err, "failed to decrease queue's front spot attribute")
	}

	dq.firstFreeFrontSpot -= 1
	return nil
}

func (dq *Deque) decreaseAttribute(attr []byte) error {
	return dq.modifyAttribute(attr, func(v int64) int64 {
		return v - 1
	})
}

func (dq *Deque) modifyAttribute(attr []byte, modifier func(int64) int64) error {
	value, err := dq.getAttribute(attr)
	if err != nil {
		return err
	}

	newValue := octosql.MonotonicMarshalInt64(modifier(int64(value)))

	err = dq.tx.Set(attr, newValue)
	if err != nil {
		return errors.Wrap(err, "failed to set new value")
	}

	return nil
}

func (dq *Deque) getAttribute(attr []byte) (int, error) {
	value, err := dq.tx.Get(attr)
	stringAttribute := string(attr)
	switch err {
	case ErrNotFound:
		value, ok := defaultValues[stringAttribute]
		if !ok {
			return 0, errors.New("this byte slice isn't a queue attribute")
		}

		err2 := dq.tx.Set(attr, octosql.MonotonicMarshalInt64(int64(value)))
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

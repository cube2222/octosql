package storage

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
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
	dequeValueKeyPrefix     = []byte("$value$")
	dequeFirstFreeFrontSpot = []byte("$first$")
	dequeFirstFreeBackSpot  = []byte("$last$")
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

// Adds new element to the front of the queue.
func (dq *Deque) PushFront(value proto.Message) error {
	if !dq.initializedFront {
		err := dq.initializeFront()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in PushFront")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value in PushFront")
	}

	byteKey := getIndexKey(dq.firstFreeFrontSpot)

	err = dq.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "failed to set value in PushFront")
	}

	err = dq.decreaseFirstFreeFrontSpot() // we move the free spot at the front to the left
	if err != nil {
		return errors.Wrap(err, "failed to decrease first free front spot in PushFront")
	}

	return nil
}

//Adds new element to the back of the queue.
func (dq *Deque) PushBack(value proto.Message) error {
	if !dq.initializedBack {
		err := dq.initializeBack()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in PushBack")
		}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't serialize given value in PushBack")
	}

	byteKey := getIndexKey(dq.firstFreeBackSpot)

	err = dq.tx.Set(byteKey, data)
	if err != nil {
		return errors.Wrap(err, "failed to set value in PushBack")
	}

	err = dq.increaseFirstFreeBackSpot() // we move the free spot at the back to the right
	if err != nil {
		return errors.Wrap(err, "failed to increase first free back spot in PushBack")
	}

	return nil
}

// Removes the first element from the queue and returns it.
// Returns ErrNotFound if the list is empty.
func (dq *Deque) PopFront(value proto.Message) error {
	// there is no need to call initialize here, since PeekFront calls initialize

	err := dq.PeekFront(value) // retrieve the value
	if err == ErrNotFound {
		return ErrNotFound
	} else if err != nil {
		return errors.Wrap(err, "failed to read first element in PopFront")
	}

	// and now we have to remove it from the deque

	key := getIndexKey(dq.firstFreeFrontSpot + 1)
	err = dq.tx.Delete(key)
	if err != nil {
		return errors.Wrap(err, "failed to delete first element in PopFront")
	}

	err = dq.increaseFirstFreeFrontSpot() // we move the first free front spot to the right
	if err != nil {
		return errors.Wrap(err, "failed to modify first free front spot in PopFront")
	}

	return nil
}

// Removes the last element from the queue and returns it.
// Returns ErrNotFound if the list is empty.
func (dq *Deque) PopBack(value proto.Message) error {
	// there is no need to call initialize here, since PeekBack calls initialize

	err := dq.PeekBack(value) // retrieve the value
	if err == ErrNotFound {
		return ErrNotFound
	} else if err != nil {
		return errors.Wrap(err, "failed to read last element in PopBack")
	}

	// and now we have to remove it from the deque

	key := getIndexKey(dq.firstFreeBackSpot - 1)
	err = dq.tx.Delete(key)
	if err != nil {
		return errors.Wrap(err, "failed to delete last element in PopBack")
	}

	err = dq.decreaseFirstFreeBackSpot() // we move the first free back spot to the left
	if err != nil {
		return errors.Wrap(err, "failed to modify first free back spot in PopBack")
	}

	return nil
}

// Returns the first element of the queue without removing it.
// Returns ErrNotFound if the queue is empty.
func (dq *Deque) PeekFront(value proto.Message) error {
	if !dq.initializedFront {
		err := dq.initializeFront()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in PeekFront")
		}
	}

	return dq.getValueAtIndex(value, dq.firstFreeFrontSpot+1)
}

// Returns the last element of the queue without removing it.
// Returns ErrNotFound if the queue is empty.
func (dq *Deque) PeekBack(value proto.Message) error {
	if !dq.initializedBack {
		err := dq.initializeBack()
		if err != nil {
			return errors.Wrap(err, "failed to initialize in PeekBack")
		}
	}

	return dq.getValueAtIndex(value, dq.firstFreeBackSpot-1)
}

// Returns the value at given index.
// We don't call initialize here, since this method is only called in methods like
// Push/Peek which call initialize themselves.
func (dq *Deque) getValueAtIndex(value proto.Message, index int) error {
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

// Clears all contents of the queue including the metadata.
func (dq *Deque) Clear() error {
	if !dq.initializedFront {
		err := dq.initializeFront()
		if err != nil {
			return errors.Wrap(err, "failed to initialize front in Clear")
		}
	}

	if !dq.initializedBack {
		err := dq.initializeBack()
		if err != nil {
			return errors.Wrap(err, "failed to initialize back in Clear")
		}
	}

	for dq.firstFreeBackSpot-dq.firstFreeFrontSpot > 1 {
		key := getIndexKey(dq.firstFreeFrontSpot + 1)
		err := dq.tx.Delete(key)

		if err != nil {
			return errors.Wrap(err, "couldn't clear the list")
		}

		dq.firstFreeFrontSpot += 1
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
	dq.firstFreeFrontSpot = defaultFirstFreeFrontSpot
	dq.firstFreeBackSpot = defaultFirstFreeBackSpot
	dq.initializedFront = false
	dq.initializedBack = false

	return nil
}

func (dq *Deque) Length() (int, error) {
	if !dq.initializedFront {
		err := dq.initializeFront()
		if err != nil {
			return 0, errors.Wrap(err, "failed to initialize front in Length")
		}
	}

	if !dq.initializedBack {
		err := dq.initializeBack()
		if err != nil {
			return 0, errors.Wrap(err, "failed to initialize back in Length")
		}
	}

	// first front > first back (ALWAYS) so we need to sub 1 (base case: front = 0, back = 1, length = 0)
	return dq.firstFreeBackSpot - dq.firstFreeFrontSpot - 1, nil
}

func (dq *Deque) GetIterator(opts ...IteratorOption) *DequeIterator {
	allOpts := []IteratorOption{WithDefault(), WithPrefix(dequeValueKeyPrefix)}
	allOpts = append(allOpts, opts...)
	it := dq.tx.Iterator(allOpts...)

	return NewDequeIterator(it)
}

func (lli *DequeIterator) Next(value proto.Message) error {
	err := lli.it.Next(value)
	if err == ErrEndOfIterator {
		return err
	} else if err != nil {
		return errors.Wrap(err, "couldn't read next element")
	}

	return nil
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

	if err == ErrNotFound {
		value, ok := defaultValues[stringAttribute]
		if !ok {
			return 0, errors.New("this byte slice isn't a queue attribute")
		}

		err := dq.tx.Set(attr, octosql.MonotonicMarshalInt64(int64(value)))
		if err != nil {
			return 0, errors.Wrapf(err, "couldn't initialize queue %s field", stringAttribute)
		}

		return value, nil
	} else if err == nil {
		i, err := octosql.MonotonicUnmarshalInt64(value)
		return int(i), err
	} else {
		return 0, errors.Wrapf(err, "couldn't read %s of queue", stringAttribute)
	}
}

package storage

import (
	"log"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
)

func TestDeque(t *testing.T) {
	prefix := "test_deque"

	store := GetTestStorage(t)
	txn := store.BeginTransaction().WithPrefix([]byte(prefix))

	queue := NewDeque(txn)

	values := []octosql.Value{
		octosql.MakeInt(0),
		octosql.MakeInt(1),
		octosql.MakeInt(2),
		octosql.MakeInt(3),
		octosql.MakeInt(4),
		octosql.MakeInt(5),
	}

	length, err := queue.Length()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, 0, length)

	//test push back and push front
	for i := 3; i < 6; i++ { //write 3 4 5
		err := queue.PushBack(&values[i])
		if err != nil {
			log.Fatal(err)
		}
	}

	length, err = queue.Length()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, 3, length)

	for i := 2; i >= 0; i-- { // write 2 1 0 so the queue becomes 0 1 2 3 4 5
		err := queue.PushFront(&values[i])
		if err != nil {
			log.Fatal(err)
		}
	}

	length, err = queue.Length()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, 6, length)

	// test if all values are there
	err = queue.isEqualTo(values)
	if err != nil {
		log.Fatal("after push back and push front: ", err)
	}

	println("PushBack test passed")

	// test the iterators
	it := queue.GetIterator()
	areEqual, err := TestDequeIterator(it, values)

	if err != nil {
		log.Fatal("get iterator: ", err)
	}

	if !areEqual {
		log.Fatal("the iterator isn't correct")
	}

	_ = it.Close()

	it = queue.GetIterator(WithReverse())
	areEqual, err = TestDequeIterator(it, reverseValues(values))

	if err != nil {
		log.Fatal("get reverse iterator: ", err)
	}

	if !areEqual {
		log.Fatal("the reverse iterator isn't correct")
	}

	_ = it.Close()

	println("Iterators test passed")

	// test PeekFront and PeekBack

	err = queue.testPeekBack(values[5])
	if err != nil {
		log.Fatal("peek back: ", err)
	}

	err = queue.testPeekFront(values[0])
	if err != nil {
		log.Fatal("peek front: ", err)
	}

	println("peek test passed")

	//test pop front and pop back
	err = queue.testPopFront(values[0], false)
	if err != nil {
		log.Fatal("pop front: ", err)
	}

	err = queue.testPopBack(values[5], false)
	if err != nil {
		log.Fatal("pop back: ", err)
	}

	err = queue.isEqualTo(values[1:5])
	if err != nil {
		log.Fatal("after pop back and pop front: ", err)
	}

	err = queue.testPopFront(values[1], false) //try to pop front again
	if err != nil {
		log.Fatal("second pop front: ", err)
	}

	err = queue.testPopBack(values[4], false) //try to pop back again
	if err != nil {
		log.Fatal("second pop back: ", err)
	}

	err = queue.PushFront(&values[1]) //try to push front after popping
	if err != nil {
		log.Fatal("push front after pop front: ", err)
	}

	err = queue.PushBack(&values[4]) //try to push back after popping
	if err != nil {
		log.Fatal("push back after pop back: ", err)
	}

	err = queue.isEqualTo(values[1:5]) // test if the values were inserted correctly
	if err != nil {
		log.Fatal("after popping and pushing: ", err)
	}

	println("pop front and pop back passed")

	length, err = queue.Length()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, 4, length)

	//try to clear the whole queue using pops (at the moment there should be 1,2,3,4 in the queue)
	for i := 0; i < 2; i++ { //warning these constants are dependant on the original values
		err = queue.testPopBack(values[4-i], false)
		if err != nil {
			log.Fatal("clearing pop back: ", err)
		}

		err = queue.testPopFront(values[1+i], false)
		if err != nil {
			log.Fatal("clearing pop front: ", err)
		}
	} //after this loop the queue should be empty

	err = queue.isEqualTo([]octosql.Value{}) //test the emptiness
	if err != nil {
		log.Fatal("the queue should be empty: ", err)
	}

	//test if pops and peeks return ErrEmptyQueue
	var value octosql.Value

	err = queue.PeekFront(&value) //peek front
	if err != ErrNotFound {
		log.Fatal("peek front should have returned ErrNotFound", err)
	}

	err = queue.PeekBack(&value) //peek back
	if err != ErrNotFound {
		log.Fatal("peek back should have returned ErrNotFound", err)
	}

	err = queue.PopFront(&value) // pop front
	if err != ErrNotFound {
		log.Fatal("pop front should have returned ErrNotFound", err)
	}

	err = queue.PopBack(&value) // pop back
	if err != ErrNotFound {
		log.Fatal("pop back should have returned ErrNotFound", err)
	}

	length, err = queue.Length()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, 0, length)

	println("peeks and pops return ErrEmptyQueue correctly")

	//insert the data again
	for i := 0; i < len(values); i++ {
		err = queue.PushBack(&values[i])
		if err != nil {
			log.Fatal("repopulation of queue: ", err)
		}
	}

	length, err = queue.Length()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, len(values), length)

	//check if initialize on a queue with some data works correctly
	secondQueue := NewDeque(txn)

	err = secondQueue.testPeekFront(values[0])
	if err != nil {
		log.Fatal("bad init peek front: ", err)
	}

	err = secondQueue.testPeekBack(values[5])
	if err != nil {
		log.Fatal("bad init peek back: ", err)
	}

	if secondQueue.firstFreeFrontSpot != 0 {
		log.Fatal("the first element index of the newly initializedFront queue should be 0")
	}

	if secondQueue.firstFreeBackSpot != len(values)+1 {
		log.Fatal("the last element index of the newly initializedFront queue should be len(values) + 1")
	}

	println("reinitialization passed")

	//check if Clear works correctly
	err = secondQueue.Clear()
	if err != nil {
		log.Fatal("clear: ", err)
	}

	//secondQueue.Print()

	err = secondQueue.isEqualTo([]octosql.Value{})
	if err != nil {
		log.Fatal("after clear, the iterator isn't empty: ", err)
	}

	_, err = txn.Get(dequeFirstFreeBackSpot)
	if err != ErrNotFound {
		log.Fatal("after clear, the last key wasn't cleared: ", err)
	}

	_, err = txn.Get(dequeFirstFreeFrontSpot)
	if err != ErrNotFound {
		log.Fatal("after clear, the first key wasn't cleared: ", err)
	}
}

func (dq *Deque) testPeekBack(expected octosql.Value) error {
	return testPeek(dq.PeekBack, expected)
}

func (dq *Deque) testPeekFront(expected octosql.Value) error {
	return testPeek(dq.PeekFront, expected)
}

func testPeek(peek func(value proto.Message) error, expected octosql.Value) error {
	var value octosql.Value

	err := peek(&value)
	if err != nil {
		return err
	}

	if !octosql.AreEqual(value, expected) {
		return errors.New("the value returned from peek isn't the expected value")
	}

	err = peek(&value)
	if err != nil {
		return err
	}

	if !octosql.AreEqual(value, expected) {
		return errors.New("peek seemed to change the state of the queue")
	}

	return nil
}

func (dq *Deque) testPopBack(expected octosql.Value, wantEmpty bool) error {
	return testPop(dq.PopBack, expected, wantEmpty)
}

func (dq *Deque) testPopFront(expected octosql.Value, wantEmpty bool) error {
	return testPop(dq.PopFront, expected, wantEmpty)
}

func testPop(pop func(value proto.Message) error, expected octosql.Value, wantEmpty bool) error {
	var value octosql.Value

	err := pop(&value)
	if err == ErrNotFound && !wantEmpty {
		return errors.New("expected a value, but the queue is empty")
	} else if err == nil && wantEmpty {
		return errors.New("expected an empty queue, but a value was returned")
	} else if err != nil {
		return errors.Wrap(err, "failed to pop the element")
	}

	if !octosql.AreEqual(value, expected) {
		return errors.New("the values aren't equal")
	}

	return nil
}

func (dq *Deque) isEqualTo(values []octosql.Value) error {
	it := dq.GetIterator()

	defer func() {
		_ = it.Close()
	}()

	isCorrect, err := TestDequeIterator(it, values)
	if err != nil {
		return err
	}

	if !isCorrect {
		return errors.New("the queue doesn't contain the expected values")
	}

	return nil
}

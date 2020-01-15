package storage

import (
	"log"
	"os"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
)

func TestLinkedList(t *testing.T) {
	prefix := "test_linked_list"
	path := "test"

	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(path)
	}()

	store := NewBadgerStorage(db)
	txn := store.BeginTransaction().WithPrefix([]byte(prefix))

	linkedList := NewDequeue(txn)

	values := []octosql.Value{
		octosql.MakeInt(1),
		octosql.MakeInt(2),
		octosql.MakeInt(3),
		octosql.MakeInt(4),
		octosql.MakeInt(5),
	}

	for i := 0; i < len(values); i++ {
		err := linkedList.PushBack(&values[i])
		if err != nil {
			log.Fatal(err)
		}
	}

	/* test if all values are there */
	iter := linkedList.GetIterator()
	areEqual, err := TestDequeueIterator(iter, values)
	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal("The iterator doesn't contain the expected values")
	}

	/* test peek */
	var value octosql.Value

	err = linkedList.PeekFront(&value)
	if err != nil {
		log.Fatal(err)
	}

	if !octosql.AreEqual(value, values[0]) {
		log.Fatal("the value returned by PeekFront() isn't the first value inserted")
	}

	err = linkedList.PeekFront(&value) //PeekFront shouldn't modify the linkedList in any way
	if err != nil {
		log.Fatal(err)
	}

	if !octosql.AreEqual(value, values[0]) {
		log.Fatal("the value returned by PeekFront() the second time isn't the first value inserted")
	}

	/* test pop */
	err = linkedList.PopFront(&value)
	if err != nil {
		log.Fatal(err)
	}

	if !octosql.AreEqual(value, values[0]) {
		log.Fatal("the value returned by PopFront() isn't the first value inserted")
	}

	_ = iter.Close() //we need to close the iterator, to be able to get the next one

	iter = linkedList.GetIterator()
	areEqual, err = TestDequeueIterator(iter, values[1:])

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal("The iterator doesn't contain the expected values")
	}

	/* test pop again but this time create a new Linked List to operate on the same data*/
	linkedList2 := NewDequeue(txn)

	err = linkedList2.PopFront(&value)
	if err != nil {
		log.Fatal(err)
	}

	if !octosql.AreEqual(value, values[1]) {
		log.Fatal("the value returned by PopFront() isn't the first value inserted")
	}

	_ = iter.Close() //we need to close the iterator, to be able to get the next one

	iter = linkedList2.GetIterator()
	areEqual, err = TestDequeueIterator(iter, values[2:])
	_ = iter.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal("The iterator doesn't contain the expected values")
	}

	/* test clear */
	err = linkedList2.Clear()
	if err != nil {
		log.Fatal(err)
	}

	/* test if linked list is actually empty */
	iter = linkedList2.GetIterator()
	areEqual, err = TestDequeueIterator(iter, []octosql.Value{})
	_ = iter.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal("The iterator should be empty")
	}

	_, err = txn.Get(dequeueLastElementKey)
	if err != badger.ErrKeyNotFound {
		log.Fatal("the linked list length element index should be empty")
	}

	_, err = txn.Get(dequeueFirstElementKey)
	if err != badger.ErrKeyNotFound {
		log.Fatal("the linked list first element should be empty")
	}

	/* we should still be able to append elements tho */
	err = linkedList2.PushBack(&values[0])
	if err != nil {
		log.Fatal(err)
	}

	iter = linkedList2.GetIterator()
	areEqual, err = TestDequeueIterator(iter, values[:1])
	_ = iter.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal("The iterator doesn't contain the expected values")
	}

	err = linkedList2.Clear()
	if err != nil {
		log.Fatal(err)
	}

	/* check if PeekFront and PopFront return ErrEmptyQueue on empty list */
	linkedList3 := NewDequeue(txn)

	err = linkedList3.PeekFront(&value)
	if err != ErrEmptyQueue {
		log.Fatal("PeekFront on an empty list should have returned ErrEmptyQueue")
	}

	err = linkedList3.PopFront(&value)
	if err != ErrEmptyQueue {
		log.Fatal("PopFront on an empty list should have returned ErrEmptyQueue")
	}
}

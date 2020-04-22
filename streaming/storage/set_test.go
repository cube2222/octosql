package storage

import (
	"log"
	"testing"
	"time"

	"github.com/cube2222/octosql"
)

func TestSet(t *testing.T) {
	prefix := []byte("set_prefix_")

	store := GetTestStorage(t)
	txn := store.BeginTransaction().WithPrefix(prefix)

	set := NewSet(txn)

	values := []octosql.Value{
		octosql.MakeInt(17),
		octosql.MakeString("aaa"),
		octosql.MakeNull(),
		octosql.MakeTime(time.Now()),
	}

	for _, value := range values {
		inserted, err := set.InsertWithConfirmation(value)
		if err != nil {
			log.Fatal(err)
		}

		if !inserted {
			log.Fatal("the value wasn't inserted, although it should've been")
		}

		contains, err := set.Contains(value)
		if err != nil {
			log.Fatal(err)
		}

		if !contains {
			log.Fatal("the set should contain the value, but it doesn't")
		}
	}

	/* test iterator correctness */
	iter := set.GetIterator()
	isCorrect, err := TestSetIteratorCorrectness(iter, values)

	if !isCorrect {
		log.Fatal("The set iterator doesn't contain all the values")
	}

	if err != nil {
		log.Fatal(err)
	}
	_ = iter.Close()

	/* test erase */
	wasErased, err := set.EraseWithConfirmation(values[0])

	if !wasErased {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	contains, err := set.Contains(values[0])
	if contains {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	wasErased, err = set.EraseWithConfirmation(values[0])

	if wasErased {
		log.Fatal("the value should not have been erased, but it was")
	}

	if err != nil {
		log.Fatal(err)
	}
	/* test iterator again after removal*/
	iter = set.GetIterator()

	isCorrect, err = TestSetIteratorCorrectness(iter, values[1:])

	if !isCorrect {
		log.Fatal("The set iterator doesn't contain all the values")
	}

	if err != nil {
		log.Fatal(err)
	}
	_ = iter.Close()

	/* test clear */
	var value octosql.Value

	err = set.Clear()
	if err != nil {
		log.Fatal(err)
	}

	iter2 := set.GetIterator()
	err = iter2.Next(&value)

	if err != ErrEndOfIterator {
		log.Fatal("expected end of iterator")
	}
}

func TestSetWithCollisions(t *testing.T) {
	prefix := []byte("set_prefix_")

	store := GetTestStorage(t)
	txn := store.BeginTransaction().WithPrefix(prefix)

	set := NewSet(txn)

	values := []octosql.Value{
		octosql.MakeInt(17),
		octosql.MakeString("aaa"),
		octosql.MakeNull(),
		octosql.MakeTime(time.Now()),
	}

	for _, value := range values {
		inserted, err := set.collisionInsert(value)
		if err != nil {
			log.Fatal(err)
		}

		if !inserted {
			log.Fatal("the value wasn't inserted, although it should've been")
		}

		contains, err := set.collisionContains(value)
		if err != nil {
			log.Fatal(err)
		}

		if !contains {
			log.Fatal("the set should contain the value, but it doesn't")
		}
	}

	/* test iterator */
	iter := set.GetIterator()

	isCorrect, err := TestSetIteratorCorrectness(iter, values)

	if !isCorrect {
		log.Fatal("The set iterator doesn't contain all the values")
	}

	if err != nil {
		log.Fatal(err)
	}
	_ = iter.Close()

	/* test erase */
	wasErased, err := set.collisionErase(values[0])

	if !wasErased {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	contains, err := set.collisionContains(values[0])
	if contains {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	wasErased, err = set.collisionErase(values[0])

	if wasErased {
		log.Fatal("the value should not have been erased, but it was")
	}

	if err != nil {
		log.Fatal(err)
	}
	/* test iterator after erasing*/
	iter = set.GetIterator()
	isCorrect, err = TestSetIteratorCorrectness(iter, values[1:])

	if !isCorrect {
		log.Fatal("The set iterator doesn't contain all the values")
	}

	if err != nil {
		log.Fatal(err)
	}

	_ = iter.Close()

	/* test clear */

	var value octosql.Value

	err = set.collisionClear()
	if err != nil {
		log.Fatal(err)
	}

	iter = set.GetIterator()

	err = iter.Next(&value)
	if err != ErrEndOfIterator {
		log.Fatal("expected end of iterator")
	}

	_ = iter.Close()

	/* basic testing with two hashes */
	for i := 0; i < 2; i++ {
		inserted, err := set.collisionInsert(values[i])
		if !inserted {
			log.Fatal("the value should've been inserted, but it wasn't")
		}

		if err != nil {
			log.Fatal(err)
		}
	}

	for j := 2; j < 4; j++ {
		inserted, err := set.collisionInsert2(values[j])
		if !inserted {
			log.Fatal("the value should've been inserted, but it wasn't")
		}

		if err != nil {
			log.Fatal(err)
		}
	}

	/* test if iterator works */
	iter = set.GetIterator()
	isCorrect, err = TestSetIteratorCorrectness(iter, values)

	if !isCorrect {
		log.Fatal("The set iterator doesn't contain all the values")
	}

	if err != nil {
		log.Fatal(err)
	}

	_ = iter.Close()
}

func (set *Set) collisionInsert(value octosql.Value) (bool, error) {
	return set.insertUsingHash(value, collisionHash)
}

func (set *Set) collisionErase(value octosql.Value) (bool, error) {
	return set.eraseUsingHash(value, collisionHash)
}

func (set *Set) collisionContains(value octosql.Value) (bool, error) {
	return set.containsUsingHash(value, collisionHash)
}

func (set *Set) collisionClear() error {
	return set.clearUsingHash(collisionHash)
}

func collisionHash(value octosql.Value) ([]byte, error) {
	return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
}

//There are two different mock hash functions, to simulate if our set
//works correctly with multiple collisions
func (set *Set) collisionInsert2(value octosql.Value) (bool, error) {
	return set.insertUsingHash(value, collisionHash2)
}

func (set *Set) collisionErase2(value octosql.Value) (bool, error) {
	return set.eraseUsingHash(value, collisionHash2)
}

func (set *Set) collisionContains2(value octosql.Value) (bool, error) {
	return set.containsUsingHash(value, collisionHash2)
}

func (set *Set) collisionClear2() error {
	return set.clearUsingHash(collisionHash2)
}

func collisionHash2(value octosql.Value) ([]byte, error) {
	return []byte{1, 0, 7, 0, 6, 0, 0, 1}, nil
}

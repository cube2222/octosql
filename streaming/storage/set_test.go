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

	set := NewMultiSet(txn)

	values := []octosql.Value{
		octosql.MakeInt(17),
		octosql.MakeString("aaa"),
		octosql.MakeNull(),
		octosql.MakeTime(time.Now()),
	}

	for _, value := range values {
		if err := set.Insert(value); err != nil {
			log.Fatal(err)
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

	if err := TestSetIteratorCorrectness(iter, values, []int{1, 1, 1, 1}); err != nil {
		log.Fatal(err)
	}
	_ = iter.Close()

	/* test erase */
	if err := set.Erase(values[0]); err != nil {
		log.Fatal(err)
	}

	contains, err := set.Contains(values[0])
	if contains {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	if err := set.Erase(values[0]); err != nil {
		log.Fatal(err)
	}
	/* test iterator again after removal*/
	iter = set.GetIterator()

	if err := TestSetIteratorCorrectness(iter, values[1:], []int{1, 1, 1}); err != nil {
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

	_ = iter2.Close()

	for i := 0; i < 4; i++ {
		for j := 0; j < 2; j++ {
			if err := set.Insert(values[i]); err != nil {
				log.Fatal(err)
			}
		}
	}

	iter = set.GetIterator()

	if err := TestSetIteratorCorrectness(iter, values, []int{2, 2, 2, 2}); err != nil {
		log.Fatal(err)
	}

	_ = iter.Close()

	if err := set.Erase(values[0]); err != nil {
		log.Fatal(err)
	}

	if err := set.Erase(values[0]); err != nil {
		log.Fatal(err)
	}

	if err := set.Erase(values[1]); err != nil {
		log.Fatal(err)
	}

	if err := set.Erase(values[2]); err != nil {
		log.Fatal(err)
	}

	if err := set.Erase(values[3]); err != nil {
		log.Fatal(err)
	}

	if err := set.Erase(values[3]); err != nil {
		log.Fatal(err)
	}

	iter = set.GetIterator()
	if err := TestSetIteratorCorrectness(iter, values[1:3], []int{1, 1}); err != nil {
		log.Fatal(err)
	}
}

func TestSetWithCollisions(t *testing.T) {
	prefix := []byte("set_prefix_")

	store := GetTestStorage(t)
	txn := store.BeginTransaction().WithPrefix(prefix)

	set := NewMultiSet(txn)

	values := []octosql.Value{
		octosql.MakeInt(17),
		octosql.MakeString("aaa"),
		octosql.MakeNull(),
		octosql.MakeTime(time.Now()),
	}

	for _, value := range values {
		if err := set.collisionInsert(value); err != nil {
			log.Fatal(err)
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

	if err := TestSetIteratorCorrectness(iter, values, []int{1, 1, 1, 1}); err != nil {
		log.Fatal(err)
	}
	_ = iter.Close()

	/* test erase */
	if err := set.collisionErase(values[0]); err != nil {
		log.Fatal(err)
	}

	contains, err := set.collisionContains(values[0])
	if contains {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	if err := set.collisionErase(values[0]); err != nil {
		log.Fatal(err)
	}
	/* test iterator after erasing*/
	iter = set.GetIterator()
	if err := TestSetIteratorCorrectness(iter, values[1:], []int{1, 1, 1}); err != nil {
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
		for j := 0; j < 10; j++ {
			if err := set.collisionInsert(values[i]); err != nil {
				log.Fatal(err)
			}
		}
	}

	for i := 2; i < 4; i++ {
		for j := 0; j < 10; j++ {
			if err := set.collisionInsert2(values[i]); err != nil {
				log.Fatal(err)
			}
		}
	}

	/* test if iterator works with collisions */
	iter = set.GetIterator()
	if err := TestSetIteratorCorrectness(iter, values, []int{10, 10, 10, 10}); err != nil {
		log.Fatal(err)
	}

	_ = iter.Close()

	for i := 0; i < 10; i++ {
		if err := set.collisionErase(values[0]); err != nil {
			log.Fatal(err)
		}
	}

	if err := set.collisionErase(values[1]); err != nil {
		log.Fatal(err)
	}

	if err := set.collisionErase2(values[2]); err != nil {
		log.Fatal(err)
	}

	iter = set.GetIterator()
	if err := TestSetIteratorCorrectness(iter, values[1:], []int{9, 9, 10}); err != nil {
		log.Fatal(err)
	}

	_ = iter.Close()

	for i := 0; i < 9; i++ {
		if err := set.collisionErase(values[1]); err != nil {
			log.Fatal(err)
		}
	}

	iter = set.GetIterator()
	if err := TestSetIteratorCorrectness(iter, values[2:], []int{9, 10}); err != nil {
		log.Fatal(err)
	}

	_ = iter.Close()
}

func (set *MultiSet) collisionInsert(value octosql.Value) error {
	return set.insertWithFunction(value, collisionHash)
}

func (set *MultiSet) collisionErase(value octosql.Value) error {
	return set.eraseWithFunction(value, collisionHash)
}

func (set *MultiSet) collisionContains(value octosql.Value) (bool, error) {
	return set.containsWithFunction(value, collisionHash)
}

func (set *MultiSet) collisionClear() error {
	return set.clearUsingHash(collisionHash)
}

func collisionHash(value octosql.Value) ([]byte, error) {
	return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
}

//There are two different mock hash functions, to simulate if our set
//works correctly with multiple collisions
func (set *MultiSet) collisionInsert2(value octosql.Value) error {
	return set.insertWithFunction(value, collisionHash2)
}

func (set *MultiSet) collisionErase2(value octosql.Value) error {
	return set.eraseWithFunction(value, collisionHash2)
}

func (set *MultiSet) collisionContains2(value octosql.Value) (bool, error) {
	return set.containsWithFunction(value, collisionHash2)
}

func (set *MultiSet) collisionClear2() error {
	return set.clearUsingHash(collisionHash2)
}

func collisionHash2(value octosql.Value) ([]byte, error) {
	return []byte{1, 0, 7, 0, 6, 0, 0, 1}, nil
}

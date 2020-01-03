package storage

import (
	"log"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
)

func TestSet(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("test_set"))
	if err != nil {
		log.Fatal(err)
	}

	prefix := []byte("set_prefix_")

	defer db.DropAll()

	store := NewBadgerStorage(db)
	txn := store.BeginTransaction().WithPrefix(prefix)

	set := NewSet(txn)

	values := []octosql.Value{
		octosql.MakeInt(17),
		octosql.MakeString("aaa"),
		octosql.MakeNull(),
		octosql.MakeTime(time.Now()),
	}

	for _, value := range values {
		inserted, err := set.Insert(value)
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

	/* test erase */
	wasErased, err := set.Erase(values[0])

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

	wasErased, err = set.Erase(values[0])

	if wasErased {
		log.Fatal("the value should not have been erased, but it was")
	}

	if err != nil {
		log.Fatal(err)
	}
	/* test iterator */

	iter := set.GetIterator()
	var value octosql.Value

	for {
		err := iter.Next(&value)

		if err == ErrEndOfIterator {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
}

func TestSetWithCollisions(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("test_set_collision"))
	if err != nil {
		log.Fatal(err)
	}

	prefix := []byte("set_prefix_")

	defer db.DropAll()

	store := NewBadgerStorage(db)
	txn := store.BeginTransaction().WithPrefix(prefix)

	set := NewSet(txn)

	values := []octosql.Value{
		octosql.MakeInt(17),
		octosql.MakeString("aaa"),
		octosql.MakeNull(),
		octosql.MakeTime(time.Now()),
	}

	for _, value := range values {
		inserted, err := set.fakeInsert(value)
		if err != nil {
			log.Fatal(err)
		}

		if !inserted {
			log.Fatal("the value wasn't inserted, although it should've been")
		}

		contains, err := set.fakeContains(value)
		if err != nil {
			log.Fatal(err)
		}

		if !contains {
			log.Fatal("the set should contain the value, but it doesn't")
		}
	}

	/* test erase */
	wasErased, err := set.fakeErase(values[0])

	if !wasErased {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	contains, err := set.fakeContains(values[0])
	if contains {
		log.Fatal("the value should've been erased, but it wasn't")
	}

	if err != nil {
		log.Fatal(err)
	}

	wasErased, err = set.fakeErase(values[0])

	if wasErased {
		log.Fatal("the value should not have been erased, but it was")
	}

	if err != nil {
		log.Fatal(err)
	}
	/* test iterator */

	iter := set.GetIterator()
	var value octosql.Value

	for {
		err := iter.Next(&value)

		if err == ErrEndOfIterator {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
}

func (set *Set) fakeInsert(value octosql.Value) (bool, error) {
	return set.insertUsingHash(value, fakeHash)
}

func (set *Set) fakeErase(value octosql.Value) (bool, error) {
	return set.eraseUsingHash(value, fakeHash)
}

func (set *Set) fakeContains(value octosql.Value) (bool, error) {
	return set.containsUsingHash(value, fakeHash)
}

func fakeHash(value octosql.Value) ([]byte, error) {
	return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
}

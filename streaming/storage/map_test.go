package storage

import (
	"log"
	"testing"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
)

func TestMap(t *testing.T) {
	prefix := []byte("map_prefix_")

	store := GetTestStorage(t)
	txn := store.BeginTransaction().WithPrefix(prefix)

	badgerMap := NewMap(txn)

	keys := []octosql.Value{
		octosql.MakeString("klucz1"),
		octosql.MakeString("klucz2"),
		octosql.MakeString("klucz3"),
		octosql.MakeString("klucz4"),
		octosql.MakeString("klucz5"),
	}

	values := []octosql.Value{
		octosql.MakeNull(),
		octosql.MakeInt(17238),
		octosql.MakeFloat(1823.1823),
		octosql.MakeTuple([]octosql.Value{
			octosql.MakeInt(1923),
			octosql.MakeString("aaaaaaaaaaaaaaaaaaaa"),
		}),
		octosql.MakeString("ala ma kota i psa i jaszczurki"),
	}

	/* test Set */
	for i := 0; i < len(keys); i++ {
		err := badgerMap.Set(&keys[i], &values[i])
		if err != nil {
			log.Fatal(err)
		}
	}

	/* test iterator */
	it := badgerMap.GetIterator()
	areEqual, err := TestMapIteratorCorrectness(it, keys, values)
	_ = it.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal(errors.Wrap(err, "the iterator isn't correct"))
	}

	/* test reverse iterator */
	it = badgerMap.GetIterator(WithReverse())
	areEqual, err = TestMapIteratorCorrectness(it, reverseValues(keys), reverseValues(values))
	_ = it.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal(errors.Wrap(err, "the iterator isn't correct"))
	}

	/* test delete */
	err = badgerMap.Delete(&keys[0])
	if err != nil {
		log.Fatal(err)
	}

	it = badgerMap.GetIterator()
	areEqual, err = TestMapIteratorCorrectness(it, keys[1:], values[1:])
	_ = it.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal(errors.Wrap(err, "the iterator isn't correct"))
	}

	/* test clear */
	err = badgerMap.Clear()
	if err != nil {
		log.Fatal(err)
	}

	it = badgerMap.GetIterator()
	areEqual, err = TestMapIteratorCorrectness(it, []octosql.Value{}, []octosql.Value{})
	_ = it.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal(errors.Wrap(err, "the iterator is not empty"))
	}

	/* test if accessing a missing key returns ErrNotFound */
	missingKey := octosql.MakeString("invalid key")
	var value octosql.Value

	err = badgerMap.Get(&missingKey, &value)
	if err != ErrNotFound {
		log.Fatal("Accessing a nonexistent key should return ErrNotFound")
	}

	/* although map doesn't store any metadata to load, check if
	it will operate normally when we create some other instance
	to insert data, and then some other instance to read the data
	*/

	bMap2 := NewMap(txn)
	bMap3 := NewMap(txn)

	for i := 0; i < len(keys); i++ {
		err := bMap2.Set(&keys[i], &values[i])
		if err != nil {
			log.Fatal(err)
		}
	}

	it = bMap3.GetIterator()
	areEqual, err = TestMapIteratorCorrectness(it, keys, values)
	_ = it.Close()

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal(errors.Wrap(err, "the iterator isn't correct"))
	}

	_ = bMap3.Clear()
}

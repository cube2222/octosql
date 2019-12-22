package storage

import (
	"log"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
)

func TestMap(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("test_map"))
	if err != nil {
		log.Fatal(err)
	}

	prefix := []byte("map_prefix_")

	defer db.DropAll()

	store := NewBadgerStorage(db)
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

	if err != nil {
		log.Fatal(err)
	}

	if !areEqual {
		log.Fatal(errors.Wrap(err, "the iterator is not empty"))
	}
}

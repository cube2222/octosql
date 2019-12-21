package streaming

import (
	"log"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
)

func TestLinkedList(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		log.Fatal(err)
	}

	defer db.DropAll()

	store := NewBadgerStorage(db)
	txn := store.BeginTransaction()

	linkedList := NewLinkedList(txn.WithPrefix([]byte("test linked list")))

	value1 := octosql.MakeInt(1)
	value2 := octosql.MakeInt(2)
	value3 := octosql.MakeInt(3)

	err = linkedList.Append(&value1)
	if err != nil {
		log.Fatal(err)
	}

	err = linkedList.Append(&value2)
	if err != nil {
		log.Fatal(err)
	}

	err = linkedList.Append(&value3)
	if err != nil {
		log.Fatal(err)
	}

	iter := linkedList.GetIterator()

	var result octosql.Value

	for {
		err = iter.Next(&result)
		if err == ErrEndOfIterator {
			return
		} else if err != nil {
			log.Fatal(err)
		}

		println(result.AsInt())
	}
}

func TestMap(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		log.Fatal(err)
	}

	defer db.DropAll()

	store := NewBadgerStorage(db)
	txn := store.BeginTransaction()

	badgerMap := NewMap(txn.WithPrefix([]byte("map_prefix_")))

	key1 := octosql.MakeString("aaa")
	value1 := octosql.MakeString("siemanko")
	err = badgerMap.Set(&key1, &value1)
	if err != nil {
		panic(err)
	}

	key2 := octosql.MakeString("bbb")
	value2 := octosql.MakeString("eluwina")
	err = badgerMap.Set(&key2, &value2)
	if err != nil {
		panic(err)
	}

	it := badgerMap.GetIterator()

	var key octosql.Value
	var val octosql.Value

	for {
		err = it.Next(&key, &val)
		if err == ErrEndOfIterator {
			return
		} else if err != nil {
			log.Fatal(err)
		}

		println(key.AsString(), val.AsString())
	}

}

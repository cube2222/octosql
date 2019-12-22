package storage

import (
	"log"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
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

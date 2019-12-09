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

	defer db.Close()

	store := NewBadgerStorage(db)
	txn := store.BeginTransaction()

	linkedList := NewLinkedList(txn.WithPrefix([]byte("test linked listy")).(*badgerTransaction))

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

	iter := linkedList.GetAll()

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

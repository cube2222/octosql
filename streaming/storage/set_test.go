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
}

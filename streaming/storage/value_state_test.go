package storage

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/dgraph-io/badger/v2"
)

func TestValueState(t *testing.T) {
	prefix := []byte("test_value_store")
	path := "test_value_store"
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

	vs := NewValueState(txn)

	values := []octosql.Value{
		octosql.MakeNull(),
		octosql.MakePhantom(),
		octosql.MakeInt(17129),
		octosql.MakeFloat(18.91824),
		octosql.MakeBool(false),
		octosql.MakeString("ala ma kota i psa"),
		octosql.MakeTime(time.Now()),
		octosql.MakeDuration(18283),
	}

	values = append(values, octosql.MakeTuple(values))
	values = append(values, octosql.MakeObject(map[string]octosql.Value{
		"a": octosql.MakeInt(19),
		"b": octosql.MakeString("i tutaj też coś dodamy"),
		"c": octosql.MakeFloat(128.2481),
	}))

	var targetValue octosql.Value

	for _, value := range values {
		err = vs.Set(&value)
		if err != nil {
			log.Fatal(err)
		}

		err = vs.Get(&targetValue)
		if err != nil {
			log.Fatal(err)
		}

		if !octosql.AreEqual(value, targetValue) {
			log.Fatal("the values aren't equal")
		}

		err = vs.Clear()
		if err != nil {
			log.Fatal(err)
		}

		if vs.Get(&targetValue) != ErrKeyNotFound {
			log.Fatal("the value state should be empty after clear")
		}
	}
}

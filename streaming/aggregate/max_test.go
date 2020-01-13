package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestMaxInt(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("max")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMaxAggregate()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectZeroValue(t, ctx, aggr, tx)
}

func TestMaxString(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("max")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMaxAggregate()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("Ala"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("Ala"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("Ala"))

	ExpectZeroValue(t, ctx, aggr, tx)
}

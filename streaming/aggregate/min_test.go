package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestMinInt(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("min")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMinAggregate()

	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectZeroValue(t, ctx, aggr, tx)
}

func TestMinString(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("min")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMinAggregate()

	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("b"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("aa"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("aa"))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("aa"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("aa"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("b"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("b"))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("b"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("dddd"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("abc"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("b"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("b"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("dddd"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("dddd"))

	ExpectZeroValue(t, ctx, aggr, tx)
}

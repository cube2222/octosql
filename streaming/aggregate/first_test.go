package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestFirstInt(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("first")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewFirstAggregate()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12)) // -> 12

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11)) // -> 12 -> 11

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13)) // -> 12 -> 11 -> 13

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12)) // -> 11 -> 13

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13)) // -> 11

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(14)) // -> 11 -> 14

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(14)) // -> 11

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(15)) // -> 11 -> 15

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11)) // -> 15

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(15))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(15)) // ->

	ExpectZeroValue(t, ctx, aggr, tx)
}

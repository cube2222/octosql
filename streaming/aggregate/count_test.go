package aggregate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestCount(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("count")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewCountAggregate()

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeTime(time.Now()), octosql.MakeFloat(123.123)}))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

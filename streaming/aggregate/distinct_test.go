package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestDistinctInt(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("distinct")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewDistinctAggregate()

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{}))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(12)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(12)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(12)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(12)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(12), octosql.MakeInt(13)}))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(11), octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{}))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(12)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(12)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(12), octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(12), octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(13)}))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{octosql.MakeInt(13)}))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTuple([]octosql.Value{}))
}

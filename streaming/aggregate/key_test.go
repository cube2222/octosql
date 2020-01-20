package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestKey(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("key")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewKeyAggregate()

	key := octosql.MakeTuple([]octosql.Value{octosql.MakeString("first_name")})

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	AddValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	// RetractValue
	RetractValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	RetractValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed
	AddValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	AddValue(t, ctx, aggr, tx, key)

	RetractValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	AddValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	RetractValue(t, ctx, aggr, tx, key)

	RetractValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)

	// Early retractions
	RetractValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)

	RetractValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, key)

	ExpectValue(t, ctx, aggr, tx, key)

	RetractValue(t, ctx, aggr, tx, key)

	ExpectZeroValue(t, ctx, aggr, tx)
}

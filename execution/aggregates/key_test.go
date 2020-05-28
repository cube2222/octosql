package aggregates

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestKey(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("key")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewKeyAggregate()

	key := octosql.MakeTuple([]octosql.Value{octosql.MakeString("first_name"), octosql.MakeString("surname"), octosql.MakeString("age")})

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

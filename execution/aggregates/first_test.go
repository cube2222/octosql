package aggregates

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestFirstInt(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("first")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

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

	// Early retractions
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)
}

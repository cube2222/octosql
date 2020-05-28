package aggregates

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestCount(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("count")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewCountAggregate()

	// Empty storage
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

	// Early retractions
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

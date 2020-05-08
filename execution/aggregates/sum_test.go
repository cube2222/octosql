package aggregates

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestSumInt(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("sum")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(15))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(27))

	// Wrong type passed
	AddValueError(t, ctx, aggr, tx, octosql.MakeDuration(1234))

	AddValueError(t, ctx, aggr, tx, octosql.MakeTime(time.Now()))

	AddValueError(t, ctx, aggr, tx, octosql.MakeFloat(123.123))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(14))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(4))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(7))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(4))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

func TestSumFloat(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("sum")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(2.0))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.2))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(0.5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(0.5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(0.222))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.345))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(0.222))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(0.222))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(0))
}

func TestSumDuration(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("sum")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(3))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(0))
}

func TestSumNasty(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("sum")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	// Elements count > 0
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(5))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(-5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValueError(t, ctx, aggr, tx, octosql.MakeFloat(1.1)) // this shouldn't work, because current sum is of type Int

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(-5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(1.1)) // now the sum is cleared so we can change type

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(1.1))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(2.5))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(-2.5))

	AddValueError(t, ctx, aggr, tx, octosql.MakeDuration(1)) // this shouldn't work, because current sum is of type Float

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(2.5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(-2.5))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(1)) // now the sum is cleared so we can change type

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(0))

	// Elements count < 0
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(-5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValueError(t, ctx, aggr, tx, octosql.MakeDuration(1)) // this shouldn't work, because current sum is of type Int

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(5))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(-5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(1)) // now the sum is cleared so we can change type

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(-2))

	RetractValueError(t, ctx, aggr, tx, octosql.MakeFloat(3.5)) // this shouldn't work, because current sum is of type Float

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(-2))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(3.5)) // now the sum is cleared so we can change type

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(3.5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(0))
}

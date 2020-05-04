package aggregates

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestMinInt(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("min")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMinAggregate()

	// Empty storage
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

	// Early retractions
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectZeroValue(t, ctx, aggr, tx)

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectZeroValue(t, ctx, aggr, tx)
}

func TestMinString(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("min")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMinAggregate()

	// Empty storage
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

func TestMinBool(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("min")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMinAggregate()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	AddValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	RetractValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	AddValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	AddValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	RetractValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	RetractValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	AddValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	RetractValue(t, ctx, aggr, tx, octosql.MakeBool(true))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	RetractValue(t, ctx, aggr, tx, octosql.MakeBool(false))

	ExpectZeroValue(t, ctx, aggr, tx)
}

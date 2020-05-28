package aggregates

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestMaxInt(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("max")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMaxAggregate()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

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

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(-1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(-1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(-1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(-1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(-1))

	ExpectZeroValue(t, ctx, aggr, tx)
}

func TestMaxString(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("max")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMaxAggregate()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("fff"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	AddValue(t, ctx, aggr, tx, octosql.MakeString("Ala"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("bcd"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("a"))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeString("Ala"))

	RetractValue(t, ctx, aggr, tx, octosql.MakeString("Ala"))

	ExpectZeroValue(t, ctx, aggr, tx)
}

func TestMaxTime(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("max")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewMaxAggregate()

	now := time.Now()

	// Empty storage
	ExpectZeroValue(t, ctx, aggr, tx)

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, 1)))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, 1)))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, 1)))

	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, 1)))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	ExpectZeroValue(t, ctx, aggr, tx)

	// Mixed + multiple values
	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	AddValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, 1)))

	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, 1)))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	RetractValue(t, ctx, aggr, tx, octosql.MakeTime(now.AddDate(0, 0, -1)))

	ExpectZeroValue(t, ctx, aggr, tx)
}

package aggregates

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestDistinctSum(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("distinct")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewDistinctAggregate(NewSumAggregate())

	assert.Equal(t, aggr.String(), "sum_distinct")

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(23))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(23))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(23))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(36))

	// Wrong type passed
	AddValueError(t, ctx, aggr, tx, octosql.MakeDuration(1234))

	AddValueError(t, ctx, aggr, tx, octosql.MakeTime(time.Now()))

	AddValueError(t, ctx, aggr, tx, octosql.MakeFloat(123.123))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(24))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(24))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(24))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(25))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(25))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

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

func TestDistinctCount(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("distinct")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewDistinctAggregate(NewCountAggregate())

	assert.Equal(t, aggr.String(), "count_distinct")

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Early retractions
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

func TestDistinctAvg(t *testing.T) {
	ctx := context.Background()

	prefix := []byte("distinct")

	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewDistinctAggregate(NewAverageAggregate())

	assert.Equal(t, aggr.String(), "avg_distinct")

	// Empty storage
	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(5.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(8.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(8.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(8.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(9.66666666))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(12.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(12.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(12.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(11))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(13.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(7.5))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(9.333333333))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(8.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Early retractions
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(321))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(321))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(123.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

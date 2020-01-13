package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var currentDistinctPrefix = []byte("$current_distinct$")

type Distinct struct {
}

func NewDistinctAggregate() *Distinct {
	return &Distinct{}
}

func (agg *Distinct) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentDistinctStorage := storage.NewMap(tx.WithPrefix(currentDistinctPrefix))

	var currentValueCount octosql.Value
	err := currentDistinctStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from distinct storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	err = currentDistinctStorage.Set(&value, &currentValueCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current value in distinct storage")
	}

	return nil
}

func (agg *Distinct) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentDistinctStorage := storage.NewMap(tx.WithPrefix(currentDistinctPrefix))

	var currentValueCount octosql.Value
	err := currentDistinctStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		return errors.Wrap(err, "attempt to retract value that doesn't exist in distinct storage")
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from distinct storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() > 0 {
		err = currentDistinctStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in distinct storage")
		}
	} else {
		err = currentDistinctStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from distinct storage")
		}
	}

	return nil
}

func (agg *Distinct) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentDistinctStorage := storage.NewMap(tx.WithPrefix(currentDistinctPrefix))

	var currentValue octosql.Value
	var currentValueCount octosql.Value

	it := currentDistinctStorage.GetIterator()
	defer func() {
		_ = it.Close()
	}()

	distinctValues := make([]octosql.Value, 0)
	var err error

	for err := it.Next(&currentValue, &currentValueCount); err == nil; err = it.Next(&currentValue, &currentValueCount) {
		distinctValues = append(distinctValues, currentValue)
	}
	if err != storage.ErrEndOfIterator {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current min from storage")
	}

	return octosql.MakeTuple(distinctValues), nil
}

func (agg *Distinct) String() string {
	return "distinct"
}

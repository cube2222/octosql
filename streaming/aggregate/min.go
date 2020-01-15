package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Min struct {
}

func NewMinAggregate() *Min {
	return &Min{}
}

var currentMinPrefix = []byte("$current_min$")

func (agg *Min) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentMinStorage := storage.NewMap(tx.WithPrefix(currentMinPrefix))

	var currentValueCount octosql.Value
	err := currentMinStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from min storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	err = currentMinStorage.Set(&value, &currentValueCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current value in min storage")
	}

	return nil
}

func (agg *Min) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentMinStorage := storage.NewMap(tx.WithPrefix(currentMinPrefix))

	var currentValueCount octosql.Value
	err := currentMinStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		return errors.Wrap(err, "attempt to retract value that doesn't exist in min storage") // TODO
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from min storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() > 0 {
		err = currentMinStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in min storage")
		}
	} else {
		err = currentMinStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from min storage")
		}
	}

	return nil
}

func (agg *Min) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentMinStorage := storage.NewMap(tx.WithPrefix(currentMinPrefix))

	var currentMin octosql.Value
	var currentMinCount octosql.Value

	it := currentMinStorage.GetIterator()
	defer func() {
		_ = it.Close()
	}()

	err := it.Next(&currentMin, &currentMinCount)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current min from storage")
	}

	return currentMin, nil
}

func (agg *Min) String() string {
	return "min"
}

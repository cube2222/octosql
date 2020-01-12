package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Max struct {
}

func NewMaxAggregate() *Max {
	return &Max{}
}

var currentMaxPrefix = []byte("$current_max$")

func (agg *Max) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentMaxStorage := storage.NewMap(tx.WithPrefix(currentMaxPrefix))

	var currentValueCount octosql.Value
	err := currentMaxStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from max storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	err = currentMaxStorage.Set(&value, &currentValueCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current value in max storage")
	}

	return nil
}

func (agg *Max) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentMaxStorage := storage.NewMap(tx.WithPrefix(currentMaxPrefix))

	var currentValueCount octosql.Value
	err := currentMaxStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		return errors.Wrap(err, "attempt to retract value that doesn't exist in max storage")
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from max storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() > 0 {
		err = currentMaxStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in max storage")
		}
	} else {
		err = currentMaxStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from max storage")
		}
	}

	return nil
}

func (agg *Max) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentMaxStorage := storage.NewMap(tx.WithPrefix(currentMaxPrefix))

	var currentMax octosql.Value
	var currentMaxCount octosql.Value

	it := currentMaxStorage.GetReverseIterator()
	defer func() {
		_ = it.Close()
	}()

	err := it.Next(&currentMax, &currentMaxCount)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current max from storage")
	}

	return currentMax, nil
}

func (agg *Max) String() string {
	return "max"
}

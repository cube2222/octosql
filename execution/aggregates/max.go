package aggregates

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

var currentMaxPrefix = []byte("$current_max$")

type Max struct {
}

func NewMaxAggregate() *Max {
	return &Max{}
}

func (agg *Max) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentMaxStorage := storage.NewMap(tx.WithPrefix(currentMaxPrefix))

	var currentValueCount octosql.Value
	err := currentMaxStorage.Get(&value, &currentValueCount)
	if err == storage.ErrNotFound {
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
	if err == storage.ErrNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from max storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() == 0 { // current value was just cleared, no need to store its count or retractions count
		err = currentMaxStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from max storage")
		}
	} else {
		err = currentMaxStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in max storage")
		}
	}

	return nil
}

func (agg *Max) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentMaxStorage := storage.NewMap(tx.WithPrefix(currentMaxPrefix))

	var currentMax octosql.Value
	var currentMaxCount octosql.Value

	it := currentMaxStorage.GetIterator(storage.WithReverse())
	defer func() {
		_ = it.Close()
	}()

	for {
		err := it.Next(&currentMax, &currentMaxCount)
		if err != nil {
			return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current max from storage")
		}

		if currentMaxCount.AsInt() > 0 {
			return currentMax, nil
		}
	}
}

func (agg *Max) String() string {
	return "max"
}

func (agg *Max) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Takes the maximum element in the group. Works with any type.")),
		),
	)
}

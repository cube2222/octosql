package aggregates

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
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
	if err == storage.ErrNotFound {
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
	if err == storage.ErrNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from min storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() == 0 { // current value was just cleared, no need to store its count or retractions count
		err = currentMinStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from min storage")
		}
	} else {
		err = currentMinStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in min storage")
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

	for {
		err := it.Next(&currentMin, &currentMinCount)
		if err != nil {
			return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current min from storage")
		}

		if currentMinCount.AsInt() > 0 {
			return currentMin, nil
		}
	}
}

func (agg *Min) String() string {
	return "min"
}

func (agg *Min) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Takes the minimum element in the group. Works with any type.")),
		),
	)
}

package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var currentFirstPrefix = []byte("$current_first$")
var currentFirstCountsPrefix = []byte("$current_first_counts$")

type First struct {
}

func NewFirstAggregate() *First {
	return &First{}
}

func (agg *First) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentFirstStorage := storage.NewDeque(tx.WithPrefix(currentFirstPrefix))
	currentFirstCountsStorage := storage.NewMap(tx.WithPrefix(currentFirstCountsPrefix))

	var currentValueCount octosql.Value
	err := currentFirstCountsStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from first storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	err = currentFirstStorage.PushBack(&value)
	if err != nil {
		return errors.Wrap(err, "couldn't push back current value to first deque")
	}

	err = currentFirstCountsStorage.Set(&value, &currentValueCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current value count in first storage")
	}

	return nil
}

func (agg *First) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentFirstCountsStorage := storage.NewMap(tx.WithPrefix(currentFirstCountsPrefix))

	var currentValueCount octosql.Value
	err := currentFirstCountsStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from first storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() == 0 { // current value was just cleared, no need to store its count or retractions count
		err = currentFirstCountsStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from first storage")
		}
	} else {
		err = currentFirstCountsStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in first storage")
		}
	}

	return nil
}

func (agg *First) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentFirstStorage := storage.NewDeque(tx.WithPrefix(currentFirstPrefix))
	currentFirstCountsStorage := storage.NewMap(tx.WithPrefix(currentFirstCountsPrefix))

	var currentFirst octosql.Value
	var currentFirstCount octosql.Value

	for {
		err := currentFirstStorage.PeekFront(&currentFirst)
		if err != nil {
			return octosql.ZeroValue(), errors.Wrap(err, "couldn't peek current first from storage")
		}

		err = currentFirstCountsStorage.Get(&currentFirst, &currentFirstCount)
		if err == storage.ErrKeyNotFound {
			currentFirstCount = octosql.MakeInt(0)
		} else if err != nil {
			return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current first count from storage")
		}

		if currentFirstCount.AsInt() > 0 { // first element actually exists
			return currentFirst, nil
		} else { // first element was retracted -> we can pop it
			err := currentFirstStorage.PopFront(&currentFirst)
			if err != nil {
				return octosql.ZeroValue(), errors.Wrap(err, "couldn't pop current first from storage")
			}
		}
	}
}

func (agg *First) String() string {
	return "first"
}

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
	currentFirstStorage := storage.NewLinkedList(tx.WithPrefix(currentFirstPrefix))
	currentFirstCountsStorage := storage.NewMap(tx.WithPrefix(currentFirstCountsPrefix))

	var currentValueCount octosql.Value
	err := currentFirstCountsStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from first storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	err = currentFirstStorage.Append(&value)
	if err != nil {
		return errors.Wrap(err, "couldn't append current value to first list")
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
		return errors.Wrap(err, "attempt to retract value that doesn't exist in first storage") // TODO
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from first storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() > 0 {
		err = currentFirstCountsStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in first storage")
		}
	} else {
		err = currentFirstCountsStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from first storage")
		}
	}

	return nil
}

func (agg *First) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentFirstStorage := storage.NewLinkedList(tx.WithPrefix(currentFirstPrefix))
	currentFirstCountsStorage := storage.NewMap(tx.WithPrefix(currentFirstCountsPrefix))

	var currentFirst octosql.Value
	var currentFirstCount octosql.Value

	for {
		err := currentFirstStorage.Peek(&currentFirst)
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
		} else { // first element was retracted -> ew can pop it
			err := currentFirstStorage.Pop(&currentFirst)
			if err != nil {
				return octosql.ZeroValue(), errors.Wrap(err, "couldn't pop current first from storage")
			}
		}
	}
}

func (agg *First) String() string {
	return "first"
}

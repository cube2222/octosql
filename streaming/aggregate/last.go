package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var currentLastPrefix = []byte("$current_last$")
var currentLastCountsPrefix = []byte("$current_last_counts$")

type Last struct {
}

func NewLastAggregate() *Last {
	return &Last{}
}

func (agg *Last) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentLastStorage := storage.NewDeque(tx.WithPrefix(currentLastPrefix))
	currentLastCountsStorage := storage.NewMap(tx.WithPrefix(currentLastCountsPrefix))

	var currentValueCount octosql.Value
	err := currentLastCountsStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from last storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	err = currentLastStorage.PushFront(&value)
	if err != nil {
		return errors.Wrap(err, "couldn't push front current value to last deque")
	}

	err = currentLastCountsStorage.Set(&value, &currentValueCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current value count in last storage")
	}

	return nil
}

func (agg *Last) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentLastCountsStorage := storage.NewMap(tx.WithPrefix(currentLastCountsPrefix))

	var currentValueCount octosql.Value
	err := currentLastCountsStorage.Get(&value, &currentValueCount)
	if err == storage.ErrKeyNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from last storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() == 0 { // current value was just cleared, no need to store its count or retractions count
		err = currentLastCountsStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from last storage")
		}
	} else {
		err = currentLastCountsStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in last storage")
		}
	}

	return nil
}

func (agg *Last) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentLastStorage := storage.NewDeque(tx.WithPrefix(currentLastPrefix))
	currentLastCountsStorage := storage.NewMap(tx.WithPrefix(currentLastCountsPrefix))

	var currentLast octosql.Value
	var currentLastCount octosql.Value

	for {
		err := currentLastStorage.PeekFront(&currentLast)
		if err != nil {
			return octosql.ZeroValue(), errors.Wrap(err, "couldn't peek current last from storage")
		}

		err = currentLastCountsStorage.Get(&currentLast, &currentLastCount)
		if err == storage.ErrKeyNotFound {
			currentLastCount = octosql.MakeInt(0)
		} else if err != nil {
			return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current last count from storage")
		}

		if currentLastCount.AsInt() > 0 { // last element actually exists
			return currentLast, nil
		} else { // last element was retracted -> we can pop it
			err := currentLastStorage.PopFront(&currentLast)
			if err != nil {
				return octosql.ZeroValue(), errors.Wrap(err, "couldn't pop current last from storage")
			}
		}
	}
}

func (agg *Last) String() string {
	return "last"
}

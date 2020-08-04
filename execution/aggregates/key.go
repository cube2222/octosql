package aggregates

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

var currentKeyPrefix = []byte("$current_key$")
var currentKeyCountsPrefix = []byte("$current_key_counts$")

type Key struct {
}

func NewKeyAggregate() *Key {
	return &Key{}
}

func (agg *Key) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentKeyStorage := storage.NewValueState(tx.WithPrefix(currentKeyPrefix))
	currentKeyCountsStorage := storage.NewValueState(tx.WithPrefix(currentKeyCountsPrefix))

	var currentKeyCount octosql.Value
	err := currentKeyCountsStorage.Get(&currentKeyCount)
	if err == storage.ErrNotFound {
		currentKeyCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current key count from storage")
	}

	currentKeyCount = octosql.MakeInt(currentKeyCount.AsInt() + 1)

	err = currentKeyCountsStorage.Set(&currentKeyCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current key count in storage")
	}

	if currentKeyCount.AsInt() == 1 {
		err = currentKeyStorage.Set(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't set current key in storage")
		}
	}

	return nil
}

func (agg *Key) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentKeyStorage := storage.NewValueState(tx.WithPrefix(currentKeyPrefix))
	currentKeyCountsStorage := storage.NewValueState(tx.WithPrefix(currentKeyCountsPrefix))

	var currentKeyCount octosql.Value
	err := currentKeyCountsStorage.Get(&currentKeyCount)
	if err == storage.ErrNotFound {
		currentKeyCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current key count from storage")
	}

	currentKeyCount = octosql.MakeInt(currentKeyCount.AsInt() - 1)

	if currentKeyCount.AsInt() == 0 { // storage was just cleared, no need to store count or retractions count
		err = currentKeyStorage.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current key from storage")
		}

		err = currentKeyCountsStorage.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current key count from storage")
		}
	} else {
		err = currentKeyCountsStorage.Set(&currentKeyCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current count in storage")
		}
	}

	return nil
}

func (agg *Key) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentKeyStorage := storage.NewValueState(tx.WithPrefix(currentKeyPrefix))
	currentKeyCountsStorage := storage.NewValueState(tx.WithPrefix(currentKeyCountsPrefix))

	var currentKeyCount octosql.Value
	err := currentKeyCountsStorage.Get(&currentKeyCount)
	if err == storage.ErrNotFound || currentKeyCount.AsInt() <= 0 {
		return octosql.ZeroValue(), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current key count from storage")
	}

	var currentKey octosql.Value
	err = currentKeyStorage.Get(&currentKey)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current key count from storage")
	}

	return currentKey, nil
}

func (agg *Key) String() string {
	return "key"
}

func (agg *Key) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Stores aggregated key")),
		),
	)
}

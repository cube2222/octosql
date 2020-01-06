package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Count struct {
}

var currentCountPrefix = []byte("$current_count$")

func (agg *Count) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	currentCount, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current count")
	}

	currentCount = octosql.MakeInt(currentCount.AsInt() + 1)

	err = currentCountStorage.Set(&currentCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current count in storage")
	}

	return nil
}

func (agg *Count) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	var currentCount octosql.Value
	err := currentCountStorage.Get(&currentCount)
	if err == storage.ErrKeyNotFound {
		return octosql.MakeInt(0), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current count from storage")
	}

	return currentCount, nil
}

func (agg *Count) String() string {
	return "count"
}

package aggregates

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

var currentCountPrefix = []byte("$current_count$")

type Count struct {
}

func NewCountAggregate() *Count {
	return &Count{}
}

func (agg *Count) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	var currentCount octosql.Value
	err := currentCountStorage.Get(&currentCount)
	if err == storage.ErrNotFound {
		currentCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current count from storage")
	}

	currentCount = octosql.MakeInt(currentCount.AsInt() + 1)

	err = currentCountStorage.Set(&currentCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current count in storage")
	}

	return nil
}

func (agg *Count) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	var currentCount octosql.Value
	err := currentCountStorage.Get(&currentCount)
	if err == storage.ErrNotFound {
		currentCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current count from storage")
	}

	currentCount = octosql.MakeInt(currentCount.AsInt() - 1)

	if currentCount.AsInt() == 0 { // storage was just cleared, no need to store count or retractions count
		err = currentCountStorage.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current count from storage")
		}
	} else {
		err = currentCountStorage.Set(&currentCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current count in storage")
		}
	}

	return nil
}

func (agg *Count) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	var currentCount octosql.Value
	err := currentCountStorage.Get(&currentCount)
	if err == storage.ErrNotFound {
		return octosql.MakeInt(0), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current count from storage")
	}

	if currentCount.AsInt() < 0 {
		return octosql.MakeInt(0), nil
	}

	return currentCount, nil
}

func (agg *Count) String() string {
	return "count"
}

func (agg *Count) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Counts elements in the group.")),
		),
	)
}

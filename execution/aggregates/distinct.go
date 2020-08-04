package aggregates

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

var currentDistinctPrefix = []byte("$current_distinct$")

type Distinct struct {
	underlying execution.Aggregate
}

func NewDistinctAggregate(aggr execution.Aggregate) *Distinct {
	return &Distinct{
		underlying: aggr,
	}
}

func (agg *Distinct) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentDistinctStorage := storage.NewMap(tx.WithPrefix(currentDistinctPrefix))

	var currentValueCount octosql.Value
	err := currentDistinctStorage.Get(&value, &currentValueCount)
	if err == storage.ErrNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from distinct storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() + 1)

	if currentValueCount.AsInt() == 1 {
		err = agg.underlying.AddValue(ctx, tx, value) // first instance of this value was added, we can pass it to underlying
		if err != nil {
			return errors.Wrap(err, "couldn't add current value to underlying in distinct storage")
		}
	}

	err = currentDistinctStorage.Set(&value, &currentValueCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current value in distinct storage")
	}

	return nil
}

func (agg *Distinct) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentDistinctStorage := storage.NewMap(tx.WithPrefix(currentDistinctPrefix))

	var currentValueCount octosql.Value
	err := currentDistinctStorage.Get(&value, &currentValueCount)
	if err == storage.ErrNotFound {
		currentValueCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current value count from distinct storage")
	}

	currentValueCount = octosql.MakeInt(currentValueCount.AsInt() - 1)

	if currentValueCount.AsInt() == 0 { // current value was just cleared, no need to store its count or retractions count
		err = agg.underlying.RetractValue(ctx, tx, value) // last instance of this value was retracted, we can retract it from underlying
		if err != nil {
			return errors.Wrap(err, "couldn't retract current value from underlying in distinct storage")
		}

		err = currentDistinctStorage.Delete(&value)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current value from distinct storage")
		}
	} else {
		err = currentDistinctStorage.Set(&value, &currentValueCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current value count in distinct storage")
		}
	}

	return nil
}

func (agg *Distinct) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentValue, err := agg.underlying.GetValue(ctx, tx)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current underlying value from distinct storage")
	}

	return currentValue, nil
}

func (agg *Distinct) String() string {
	return fmt.Sprintf("%s_distinct", agg.underlying.String())
}

func (agg *Distinct) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text(fmt.Sprintf("Works like [%s](#%s), only taking into account distinct elements per group.", agg.underlying.String(), agg.underlying.String()))),
		),
	)
}

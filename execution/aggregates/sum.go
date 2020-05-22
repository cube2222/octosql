package aggregates

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

var currentSumPrefix = []byte("$current_sum$")

// this tells us whether we have active sum (of >1 elements) or not
// (so we can to distinguish neutral element and currentSum = 0)
var currentSumCountPrefix = []byte("$current_sum_count$")

type Sum struct {
}

func NewSumAggregate() *Sum {
	return &Sum{}
}

func (agg *Sum) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentSumStorage := storage.NewValueState(tx.WithPrefix(currentSumPrefix))
	currentSumCountStorage := storage.NewValueState(tx.WithPrefix(currentSumCountPrefix))

	valueType := value.GetType()

	currentSum, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum from storage")
	}

	var currentCount octosql.Value
	err = currentSumCountStorage.Get(&currentCount)
	if err == storage.ErrNotFound {
		currentCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current sum elements count from storage")
	}

	if isNeutralElement(currentSum, currentCount) {
		currentSum, err = getAppropriateNeutralElement(valueType)
		if err != nil {
			return errors.Wrap(err, "couldn't get neutral element for sum")
		}
	}

	if valueType != currentSum.GetType() {
		return errors.Errorf("type of value passed (%s) doesn't match current sum type (%s)", value.GetType(), currentSum.GetType())
	}

	switch valueType {
	case octosql.TypeInt:
		currentSum = octosql.MakeInt(currentSum.AsInt() + value.AsInt())
	case octosql.TypeFloat:
		currentSum = octosql.MakeFloat(currentSum.AsFloat() + value.AsFloat())
	case octosql.TypeDuration:
		currentSum = octosql.MakeDuration(currentSum.AsDuration() + value.AsDuration())
	default:
		return errors.Errorf("unsupported value type passed to sum: %s", valueType)
	}

	currentCount = octosql.MakeInt(currentCount.AsInt() + 1)

	err = currentSumStorage.Set(&currentSum)
	if err != nil {
		return errors.Wrap(err, "couldn't set current sum in storage in storage")
	}

	err = currentSumCountStorage.Set(&currentCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current sum elements count in storage")
	}

	return nil
}

func isNeutralElement(currentSum octosql.Value, currentCount octosql.Value) bool {
	switch currentSum.GetType() {
	case octosql.TypeInt:
		return currentSum.AsInt() == 0 && currentCount.AsInt() == 0
	case octosql.TypeFloat:
		return currentSum.AsFloat() == 0.0 && currentCount.AsInt() == 0
	case octosql.TypeDuration:
		return currentSum.AsDuration() == 0 && currentCount.AsInt() == 0
	default:
		panic("unreachable")
	}
}

func getAppropriateNeutralElement(valueType octosql.Type) (octosql.Value, error) {
	switch valueType {
	case octosql.TypeInt:
		return octosql.MakeInt(0), nil
	case octosql.TypeFloat:
		return octosql.MakeFloat(0.0), nil
	case octosql.TypeDuration:
		return octosql.MakeDuration(0), nil
	default:
		return octosql.ZeroValue(), errors.Errorf("unsupported value type passed to sum: %s", valueType)
	}
}

func (agg *Sum) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentSumPrefix := storage.NewValueState(tx.WithPrefix(currentSumPrefix))
	currentSumCountStorage := storage.NewValueState(tx.WithPrefix(currentSumCountPrefix))

	valueType := value.GetType()

	currentSum, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum from storage")
	}

	var currentCount octosql.Value
	err = currentSumCountStorage.Get(&currentCount)
	if err == storage.ErrNotFound {
		currentCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current sum elements count from storage")
	}

	if isNeutralElement(currentSum, currentCount) {
		currentSum, err = getAppropriateNeutralElement(valueType)
		if err != nil {
			return errors.Wrap(err, "couldn't get neutral element for sum")
		}
	}

	if valueType != currentSum.GetType() {
		return errors.Errorf("type of value passed (%s) doesn't match current sum type (%s)", value.GetType(), currentSum.GetType())
	}

	switch valueType {
	case octosql.TypeInt:
		currentSum = octosql.MakeInt(currentSum.AsInt() - value.AsInt())
	case octosql.TypeFloat:
		currentSum = octosql.MakeFloat(currentSum.AsFloat() - value.AsFloat())
	case octosql.TypeDuration:
		currentSum = octosql.MakeDuration(currentSum.AsDuration() - value.AsDuration())
	default:
		return errors.Errorf("unsupported value type passed to sum: %s", valueType)
	}

	currentCount = octosql.MakeInt(currentCount.AsInt() - 1)

	err = currentSumPrefix.Set(&currentSum)
	if err != nil {
		return errors.Wrap(err, "couldn't set current sum in storage")
	}

	if currentCount.AsInt() == 0 {
		err = currentSumCountStorage.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current sum elements count from storage")
		}
	} else {
		err = currentSumCountStorage.Set(&currentCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set current sum elements count in storage")
		}
	}

	return nil
}

func (agg *Sum) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentSumStorage := storage.NewValueState(tx.WithPrefix(currentSumPrefix))

	var currentSum octosql.Value
	err := currentSumStorage.Get(&currentSum)
	if err == storage.ErrNotFound {
		return octosql.MakeInt(0), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current sum from storage")
	}

	return currentSum, nil
}

func (agg *Sum) String() string {
	return "sum"
}

func (agg *Sum) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Sums Floats, Ints or Durations in the group. You may not mix types.")),
		),
	)
}

package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var currentSumPrefix = []byte("$current_sum$")

type Sum struct {
}

func NewSumAggregate() *Sum {
	return &Sum{}
}

func (agg *Sum) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentSumStorage := storage.NewValueState(tx.WithPrefix(currentSumPrefix))

	currentSum, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum")
	}

	if isNeutralElement(currentSum) {
		currentSum, err = getAppropriateNeutralElement(value.GetType())
		if err != nil {
			return errors.Wrap(err, "couldn't get neutral element for sum")
		}
	}

	switch valueType := value.GetType(); valueType {
	case octosql.TypeInt:
		currentSum = octosql.MakeInt(currentSum.AsInt() + value.AsInt())
	case octosql.TypeFloat:
		currentSum = octosql.MakeFloat(currentSum.AsFloat() + value.AsFloat())
	case octosql.TypeDuration:
		currentSum = octosql.MakeDuration(currentSum.AsDuration() + value.AsDuration())
	default:
		return errors.Errorf("unsupported value type passed to sum: %s", valueType)
	}

	err = currentSumStorage.Set(&currentSum)
	if err != nil {
		return errors.Wrap(err, "couldn't set current sum in storage")
	}

	return nil
}

func isNeutralElement(currentSum octosql.Value) bool {
	return currentSum.GetType() == octosql.TypeInt && currentSum.AsInt() == 0
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

	currentSum, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum")
	}

	clearSum := true

	switch valueType := value.GetType(); valueType {
	case octosql.TypeInt:
		currentSum = octosql.MakeInt(currentSum.AsInt() - value.AsInt())

		if currentSum.AsInt() > 0 {
			clearSum = false
		}
	case octosql.TypeFloat:
		currentSum = octosql.MakeFloat(currentSum.AsFloat() - value.AsFloat())

		if currentSum.AsFloat() > 0.0 {
			clearSum = false
		}
	case octosql.TypeDuration:
		currentSum = octosql.MakeDuration(currentSum.AsDuration() - value.AsDuration())

		if currentSum.AsDuration() > 0 {
			clearSum = false
		}
	default:
		return errors.Errorf("unsupported value type passed to sum: %s", valueType)
	}

	if !clearSum {
		err = currentSumPrefix.Set(&currentSum)
		if err != nil {
			return errors.Wrap(err, "couldn't set current sum in storage")
		}
	} else {
		err = currentSumPrefix.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current sum in storage")
		}
	}

	return nil
}

func (agg *Sum) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentSumStorage := storage.NewValueState(tx.WithPrefix(currentSumPrefix))

	var currentSum octosql.Value
	err := currentSumStorage.Get(&currentSum)
	if err == storage.ErrKeyNotFound {
		return octosql.MakeInt(0), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current sum from storage")
	}

	return currentSum, nil
}

func (agg *Sum) String() string {
	return "sum"
}

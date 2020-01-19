package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Average struct {
	underlyingSum   *Sum
	underlyingCount *Count
}

func NewAverageAggregate() *Average {
	return &Average{
		underlyingSum:   NewSumAggregate(),
		underlyingCount: NewCountAggregate(),
	}
}

func (agg *Average) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	valueType := value.GetType()

	if !isAppropriateType(valueType) {
		return errors.Errorf("type of value passed (%s) isn't appropriate for calculating average", valueType)
	}

	err := agg.underlyingSum.AddValue(ctx, tx, value)
	if err != nil {
		return errors.Wrap(err, "couldn't add value to sum for avg")
	}

	err = agg.underlyingCount.AddValue(ctx, tx, value)
	if err != nil {
		err2 := agg.underlyingSum.RetractValue(ctx, tx, value) // TODO - is this necessary
		if err2 != nil {
			return errors.Wrap(err2, "couldn't retract value from sum for avg after error occured in count update")
		}

		return errors.Wrap(err, "couldn't add element to elements count for avg")
	}

	return nil
}

func isAppropriateType(valueType octosql.Type) bool {
	return valueType == octosql.TypeInt || valueType == octosql.TypeFloat
}

func (agg *Average) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	valueType := value.GetType()

	if !isAppropriateType(valueType) {
		return errors.Errorf("type of value passed (%s) isn't appropriate for calculating average", valueType)
	}

	err := agg.underlyingSum.RetractValue(ctx, tx, value)
	if err != nil {
		return errors.Wrap(err, "couldn't retract value from sum for avg")
	}

	err = agg.underlyingCount.RetractValue(ctx, tx, value)
	if err != nil {
		err2 := agg.underlyingSum.AddValue(ctx, tx, value) // TODO - is this necessary
		if err2 != nil {
			return errors.Wrap(err2, "couldn't add value to sum for avg after error occured in count update")
		}

		return errors.Wrap(err, "couldn't add element to elements count for avg")
	}

	return nil
}

func (agg *Average) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentSum, err := agg.underlyingSum.GetValue(ctx, tx)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current sum for avg")
	}

	currentCount, err := agg.underlyingCount.GetValue(ctx, tx)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current elements count for avg")
	}

	if currentCount.AsInt() <= 0 { // no values or early retractions
		return octosql.MakeInt(0), nil // TODO - I don't think returning anything other than "neutral element" would be better
	}

	var currentAvg octosql.Value

	switch currentSum.GetType() {
	case octosql.TypeInt:
		currentAvg = octosql.MakeFloat(float64(currentSum.AsInt()) / float64(currentCount.AsInt()))
	case octosql.TypeFloat:
		currentAvg = octosql.MakeFloat(currentSum.AsFloat() / float64(currentCount.AsInt()))
	default:
		panic("unreachable")
	}

	return currentAvg, nil
}

func (agg *Average) String() string {
	return "avg"
}

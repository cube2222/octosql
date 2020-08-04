package aggregates

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
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
		return errors.Wrap(err, "couldn't add element to elements count for avg")
	}

	return nil
}

func isAppropriateType(valueType octosql.Type) bool {
	return valueType == octosql.TypeInt || valueType == octosql.TypeFloat || valueType == octosql.TypeDuration
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
		return octosql.MakeInt(0), nil
	}

	var currentAvg octosql.Value

	switch currentSum.GetType() {
	case octosql.TypeInt:
		currentAvg = octosql.MakeFloat(float64(currentSum.AsInt()) / float64(currentCount.AsInt()))
	case octosql.TypeFloat:
		currentAvg = octosql.MakeFloat(currentSum.AsFloat() / float64(currentCount.AsInt()))
	case octosql.TypeDuration:
		currentAvg = octosql.MakeDuration(currentSum.AsDuration() / time.Duration(currentCount.AsInt()))
	default:
		panic("unreachable")
	}

	return currentAvg, nil
}

func (agg *Average) String() string {
	return "avg"
}

func (agg *Average) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text(fmt.Sprintf("Works like [%s](#%s) and [%s](#%s) combined", agg.underlyingSum.String(), agg.underlyingSum.String(), agg.underlyingCount.String(), agg.underlyingCount.String()))),
		),
	)
}

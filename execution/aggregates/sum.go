package aggregates

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Sum struct {
	sums       *execution.HashMap
	typedValue octosql.Value
}

func NewSum() *Sum {
	return &Sum{
		sums: execution.NewHashMap(),
	}
}

func (agg *Sum) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Sums Floats, Ints or Durations in the group. You may not mix types.")),
		),
	)
}

func (agg *Sum) AddRecord(key octosql.Value, value octosql.Value) error {
	sum, previousValueExists, err := agg.sums.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum out of hashmap")
	}
	octoSum := sum.(octosql.Value)

	if octosql.AreEqual(agg.typedValue, octosql.ZeroValue()) {
		agg.typedValue = value
	}

	if agg.typedValue.GetType() != value.GetType() {
		return errors.Errorf("mixed types in avg: %v and %v with values %v and %v",
			value.GetType(), agg.typedValue.GetType(),
			value, agg.typedValue)
	}

	switch value.GetType() {
	case octosql.TypeInt:
		if previousValueExists {
			octoSum = octosql.MakeInt(octoSum.AsInt() + value.AsInt())
		} else {
			octoSum = value
		}

	case octosql.TypeDuration:
		if previousValueExists {
			octoSum = octosql.MakeDuration(octoSum.AsDuration() + value.AsDuration())
		} else {
			octoSum = value
		}

	case octosql.TypeFloat:
		if previousValueExists {
			octoSum = octosql.MakeFloat(octoSum.AsFloat() + value.AsFloat())
		} else {
			octoSum = value
		}

	default:
		return errors.Errorf("invalid type in sum: %v with value %v", value.GetType(), value)
	}

	err = agg.sums.Set(key, sum)
	if err != nil {
		return errors.Wrap(err, "couldn't put new sum into hashmap")
	}

	return nil
}

func (agg *Sum) GetAggregated(key octosql.Value) (octosql.Value, error) {
	sum, ok, err := agg.sums.Get(key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get sum out of hashmap")
	}

	if !ok {
		return octosql.ZeroValue(), errors.Errorf("sum for key not found")
	}

	return sum.(octosql.Value), nil
}

func (agg *Sum) String() string {
	return "sum"
}

package aggregates

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Max struct {
	maxes      *execution.HashMap
	typedValue octosql.Value
}

func NewMax() *Max {
	return &Max{
		maxes: execution.NewHashMap(),
	}
}

func (agg *Max) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Takes the maximum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations.")),
		),
	)
}

func (agg *Max) AddRecord(key octosql.Value, value octosql.Value) error {
	max, previousValueExists, err := agg.maxes.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current max out of hashmap")
	}
	octoMax := max.(octosql.Value)

	if octosql.AreEqual(agg.typedValue, octosql.ZeroValue()) {
		agg.typedValue = value
	}

	if !previousValueExists {
		octoMax = value
	} else {
		cmp, err := octosql.Compare(value, octoMax)
		if err != nil {
			return errors.Wrap(err, "couldn't compare current max with new value")
		}
		if cmp == octosql.GreaterThan {
			octoMax = value
		}
	}

	err = agg.maxes.Set(key, octoMax)
	if err != nil {
		return errors.Wrap(err, "couldn't put new max into hashmap")
	}

	return nil
}

func (agg *Max) GetAggregated(key octosql.Value) (octosql.Value, error) {
	max, ok, err := agg.maxes.Get(key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get max out of hashmap")
	}

	if !ok {
		return octosql.ZeroValue(), errors.Errorf("max for key not found")
	}

	return max.(octosql.Value), nil
}

func (agg *Max) String() string {
	return "max"
}

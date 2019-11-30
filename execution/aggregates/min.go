package aggregates

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Min struct {
	mins       *execution.HashMap
	typedValue octosql.Value
}

func NewMin() *Min {
	return &Min{
		mins: execution.NewHashMap(),
	}
}

func (agg *Min) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Takes the minimum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations.")),
		),
	)
}

func (agg *Min) AddRecord(key octosql.Value, value octosql.Value) error {
	min, previousValueExists, err := agg.mins.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current min out of hashmap")
	}
	octoMin := min.(octosql.Value)

	if octosql.AreEqual(agg.typedValue, octosql.ZeroValue()) {
		agg.typedValue = value
	}

	if !previousValueExists {
		octoMin = value
	} else {
		cmp, err := octosql.Compare(value, octoMin)
		if err != nil {
			return errors.Wrap(err, "couldn't compare current min with new value")
		}
		if cmp == octosql.GreaterThan {
			octoMin = value
		}
	}

	err = agg.mins.Set(key, octoMin)
	if err != nil {
		return errors.Wrap(err, "couldn't put new min into hashmap")
	}

	return nil
}

func (agg *Min) GetAggregated(key octosql.Value) (octosql.Value, error) {
	min, ok, err := agg.mins.Get(key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get min out of hashmap")
	}

	if !ok {
		return octosql.ZeroValue(), errors.Errorf("min for key not found")
	}

	return min.(octosql.Value), nil
}

func (agg *Min) String() string {
	return "min"
}

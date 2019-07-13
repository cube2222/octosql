package aggregates

import (
	"github.com/cube2222/octosql"
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

func (agg *Min) AddRecord(key octosql.Tuple, value octosql.Value) error {
	min, previousValueExists, err := agg.mins.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current min out of hashmap")
	}

	if agg.typedValue == nil {
		agg.typedValue = value
	}
	switch value := value.(type) {
	case octosql.Int:
		_, typeOk := agg.typedValue.(octosql.Int)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !previousValueExists || value < min.(octosql.Int) {
			min = value
		}

	case octosql.Float:
		_, typeOk := agg.typedValue.(octosql.Float)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !previousValueExists || value < min.(octosql.Float) {
			min = value
		}

	case octosql.String:
		_, typeOk := agg.typedValue.(octosql.String)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !previousValueExists || value < min.(octosql.String) {
			min = value
		}

	case octosql.Bool:
		_, typeOk := agg.typedValue.(octosql.Bool)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !previousValueExists || value == false {
			min = value
		}

	case octosql.Time:
		_, typeOk := agg.typedValue.(octosql.Time)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !previousValueExists || value.AsTime().Before(min.(octosql.Time).AsTime()) {
			min = value
		}

	default:
		return errors.Errorf("invalid type in min: %v with value %v", execution.GetType(value), value)
	}

	err = agg.mins.Set(key, min)
	if err != nil {
		return errors.Wrap(err, "couldn't put new min into hashmap")
	}

	return nil
}

func (agg *Min) GetAggregated(key octosql.Tuple) (octosql.Value, error) {
	min, ok, err := agg.mins.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get min out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("min for key not found")
	}

	return min.(octosql.Value), nil
}

func (agg *Min) String() string {
	return "min"
}

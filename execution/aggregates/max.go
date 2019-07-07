package aggregates

import (
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Max struct {
	maxes      *execution.HashMap
	typedValue interface{}
}

func NewMax() *Max {
	return &Max{
		maxes: execution.NewHashMap(),
	}
}

func (agg *Max) AddRecord(key []interface{}, value interface{}) error {
	max, ok, err := agg.maxes.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current max out of hashmap")
	}

	if agg.typedValue == nil {
		agg.typedValue = value
	}
	switch value := value.(type) {
	case int:
		_, typeOk := agg.typedValue.(int)
		if !typeOk {
			return errors.Errorf("mixed types in max: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value > max.(int) {
			max = value
		}

	case float64:
		_, typeOk := agg.typedValue.(float64)
		if !typeOk {
			return errors.Errorf("mixed types in max: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value > max.(float64) {
			max = value
		}

	case string:
		_, typeOk := agg.typedValue.(string)
		if !typeOk {
			return errors.Errorf("mixed types in max: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value > max.(string) {
			max = value
		}

	case bool:
		_, typeOk := agg.typedValue.(bool)
		if !typeOk {
			return errors.Errorf("mixed types in max: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value == true {
			max = value
		}

	case time.Time:
		_, typeOk := agg.typedValue.(time.Time)
		if !typeOk {
			return errors.Errorf("mixed types in max: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value.After(max.(time.Time)) {
			max = value
		}
	default:
		return errors.Errorf("invalid type in max: %v with value %v", execution.GetType(value), value)
	}

	err = agg.maxes.Set(key, max)
	if err != nil {
		return errors.Wrap(err, "couldn't put new max into hashmap")
	}

	return nil
}

func (agg *Max) GetAggregated(key []interface{}) (interface{}, error) {
	max, ok, err := agg.maxes.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get max out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("max for key not found")
	}

	return max, nil
}

func (agg *Max) String() string {
	return "max"
}

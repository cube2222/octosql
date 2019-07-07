package aggregates

import (
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Min struct {
	mins       *execution.HashMap
	typedValue interface{}
}

func NewMin() *Min {
	return &Min{
		mins: execution.NewHashMap(),
	}
}

func (agg *Min) AddRecord(key []interface{}, value interface{}) error {
	min, ok, err := agg.mins.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current min out of hashmap")
	}

	if agg.typedValue == nil {
		agg.typedValue = value
	}
	switch value := value.(type) {
	case int:
		_, typeOk := agg.typedValue.(int)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value < min.(int) {
			min = value
		}

	case float64:
		_, typeOk := agg.typedValue.(float64)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value < min.(float64) {
			min = value
		}

	case string:
		_, typeOk := agg.typedValue.(string)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value < min.(string) {
			min = value
		}

	case bool:
		_, typeOk := agg.typedValue.(bool)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value == false {
			min = value
		}

	case time.Time:
		_, typeOk := agg.typedValue.(time.Time)
		if !typeOk {
			return errors.Errorf("mixed types in min: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if !ok || value.Before(min.(time.Time)) {
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

func (agg *Min) GetAggregated(key []interface{}) (interface{}, error) {
	min, ok, err := agg.mins.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get min out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("min for key not found")
	}

	return min, nil
}

func (agg *Min) String() string {
	return "min"
}

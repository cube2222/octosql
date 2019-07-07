package aggregates

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Sum struct {
	sums       *execution.HashMap
	typedValue interface{}
}

func NewSum() *Sum {
	return &Sum{
		sums: execution.NewHashMap(),
	}
}

func (agg *Sum) AddRecord(key []interface{}, value interface{}) error {
	sum, ok, err := agg.sums.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum out of hashmap")
	}

	if agg.typedValue == nil {
		agg.typedValue = value
	}
	switch value := value.(type) {
	case int:
		_, typeOk := agg.typedValue.(int)
		if !typeOk {
			return errors.Errorf("mixed types in sum: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if ok {
			sum = sum.(int) + value
		} else {
			sum = value
		}

	case float64:
		_, typeOk := agg.typedValue.(float64)
		if !typeOk {
			return errors.Errorf("mixed types in sum: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if ok {
			sum = sum.(float64) + value
		} else {
			sum = value
		}

	default:
		return errors.Errorf("invalid type in sum: %v with value %v", execution.GetType(value), value)
	}

	err = agg.sums.Set(key, sum)
	if err != nil {
		return errors.Wrap(err, "couldn't put new sum into hashmap")
	}

	return nil
}

func (agg *Sum) GetAggregated(key []interface{}) (interface{}, error) {
	sum, ok, err := agg.sums.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get sum out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("sum for key not found")
	}

	return sum, nil
}

func (agg *Sum) String() string {
	return "sum"
}

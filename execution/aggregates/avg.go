package aggregates

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Average struct {
	averages   *execution.HashMap
	counts     *execution.HashMap
	typedValue interface{}
}

func NewAverage() *Average {
	return &Average{
		averages: execution.NewHashMap(),
		counts:   execution.NewHashMap(),
	}
}

func (agg *Average) AddRecord(key []interface{}, value interface{}) error {
	if agg.typedValue == nil {
		agg.typedValue = value
	}

	var floatValue float64
	switch value := value.(type) {
	case float64:
		_, typeOk := agg.typedValue.(float64)
		if !typeOk {
			return errors.Errorf("mixed types in avg: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}
		floatValue = value
	case int:
		_, typeOk := agg.typedValue.(int)
		if !typeOk {
			return errors.Errorf("mixed types in avg: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}
		floatValue = float64(value)
	default:
		return errors.Errorf("invalid type in average: %v with value %v", execution.GetType(value), value)
	}

	count, ok, err := agg.counts.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current element count out of hashmap")
	}

	average, ok, err := agg.averages.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current average out of hashmap")
	}

	var newAverage float64
	var newCount int
	if ok {
		newCount = count.(int) + 1
		newAverage = (average.(float64)*float64(newCount-1) + floatValue) / float64(newCount)
	} else {
		newCount = 1
		newAverage = floatValue
	}

	err = agg.counts.Set(key, newCount)
	if err != nil {
		return errors.Wrap(err, "couldn't put new element count into hashmap")
	}

	err = agg.averages.Set(key, newAverage)
	if err != nil {
		return errors.Wrap(err, "couldn't put new average into hashmap")
	}

	return nil
}

func (agg *Average) GetAggregated(key []interface{}) (interface{}, error) {
	average, ok, err := agg.averages.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get average out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("average for key not found")
	}

	return average, nil
}

func (agg *Average) String() string {
	return "avg"
}

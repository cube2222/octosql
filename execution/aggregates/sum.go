package aggregates

import (
	"github.com/cube2222/octosql"
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

func (agg *Sum) AddRecord(key octosql.Tuple, value octosql.Value) error {
	sum, ok, err := agg.sums.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current sum out of hashmap")
	}

	if agg.typedValue == nil {
		agg.typedValue = value
	}
	switch value := value.(type) {
	case octosql.Int:
		_, typeOk := agg.typedValue.(octosql.Int)
		if !typeOk {
			return errors.Errorf("mixed types in sum: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if ok {
			sum = sum.(octosql.Int) + value
		} else {
			sum = value
		}

	case octosql.Float:
		_, typeOk := agg.typedValue.(octosql.Float)
		if !typeOk {
			return errors.Errorf("mixed types in sum: %v and %v with values %v and %v",
				execution.GetType(value), execution.GetType(agg.typedValue),
				value, agg.typedValue)
		}

		if ok {
			sum = sum.(octosql.Float) + value
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

func (agg *Sum) GetAggregated(key octosql.Tuple) (octosql.Value, error) {
	sum, ok, err := agg.sums.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get sum out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("sum for key not found")
	}

	return sum.(octosql.Value), nil
}

func (agg *Sum) String() string {
	return "sum"
}

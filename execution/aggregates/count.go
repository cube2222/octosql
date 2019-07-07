package aggregates

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Count struct {
	counts *execution.HashMap
}

func NewCount() *Count {
	return &Count{
		counts: execution.NewHashMap(),
	}
}

func (agg *Count) AddRecord(key []interface{}, value interface{}) error {
	count, ok, err := agg.counts.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current count out of hashmap")
	}

	var newCount int
	if ok {
		newCount = count.(int) + 1
	} else {
		newCount = 1
	}

	err = agg.counts.Set(key, newCount)
	if err != nil {
		return errors.Wrap(err, "couldn't put new count into hashmap")
	}

	return nil
}

func (agg *Count) GetAggregated(key []interface{}) (interface{}, error) {
	count, ok, err := agg.counts.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get count out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("count for key not found")
	}

	return count, nil
}

func (agg *Count) String() string {
	return "count"
}

package aggregates

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type First struct {
	firsts *execution.HashMap
}

func NewFirst() *First {
	return &First{
		firsts: execution.NewHashMap(),
	}
}

func (agg *First) AddRecord(key []interface{}, value interface{}) error {
	_, ok, err := agg.firsts.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current first out of hashmap")
	}

	if ok {
		return nil
	}

	err = agg.firsts.Set(key, value)
	if err != nil {
		return errors.Wrap(err, "couldn't put new first into hashmap")
	}

	return nil
}

func (agg *First) GetAggregated(key []interface{}) (interface{}, error) {
	first, ok, err := agg.firsts.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get first out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("first for key not found")
	}

	return first, nil
}

func (agg *First) String() string {
	return "first"
}

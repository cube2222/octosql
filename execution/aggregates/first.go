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

func (c *First) AddRecord(key []interface{}, value interface{}) error {
	_, ok, err := c.firsts.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current first out of hashmap")
	}

	if ok {
		return nil
	}

	err = c.firsts.Set(key, value)
	if err != nil {
		return errors.Wrap(err, "couldn't put new first into hashmap")
	}

	return nil
}

func (c *First) GetAggregated(key []interface{}) (interface{}, error) {
	first, ok, err := c.firsts.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get count out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("count for key not found")
	}

	return first, nil
}

func (c *First) String() string {
	return "first"
}

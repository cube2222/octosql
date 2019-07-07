package aggregates

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Last struct {
	lasts *execution.HashMap
}

func NewLast() *Last {
	return &Last{
		lasts: execution.NewHashMap(),
	}
}

func (agg *Last) AddRecord(key []interface{}, value interface{}) error {
	err := agg.lasts.Set(key, value)
	if err != nil {
		return errors.Wrap(err, "couldn't put new last into hashmap")
	}

	return nil
}

func (agg *Last) GetAggregated(key []interface{}) (interface{}, error) {
	last, ok, err := agg.lasts.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get last out of hashmap")
	}

	if !ok {
		return nil, errors.Errorf("last for key not found")
	}

	return last, nil
}

func (agg *Last) String() string {
	return "last"
}

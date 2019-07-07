package aggregates

import (
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Distinct struct {
	underlying execution.Aggregate
	groupSets  *execution.HashMap
}

func NewDistinct(underlying execution.Aggregate) *Distinct {
	return &Distinct{
		underlying: underlying,
		groupSets:  execution.NewHashMap(),
	}
}

func (agg *Distinct) AddRecord(key []interface{}, value interface{}) error {
	groupSet, ok, err := agg.groupSets.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get distinct value set for group key")
	}

	var newGroupSet *execution.HashMap
	if !ok {
		newGroupSet = execution.NewHashMap()
	} else {
		newGroupSet = groupSet.(*execution.HashMap)
	}

	_, ok, err = newGroupSet.Get(value)
	if err != nil {
		return errors.Wrap(err, "couldn't get value out of distinct value set for given key")
	}

	if ok {
		// This value has been here already
		return nil
	}

	err = agg.underlying.AddRecord(key, value)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to underlying aggregate")
	}

	err = newGroupSet.Set(value, struct{}{})
	if err != nil {
		return errors.Wrap(err, "couldn't add value to distinct value set for given key")
	}

	err = agg.groupSets.Set(key, newGroupSet)
	if err != nil {
		return errors.Wrap(err, "couldn't save distinct value set for given key")
	}

	return nil
}

func (agg *Distinct) GetAggregated(key []interface{}) (interface{}, error) {
	return agg.underlying.GetAggregated(key)
}

func (agg *Distinct) String() string {
	return fmt.Sprintf("%s_distinct", agg.underlying.String())
}

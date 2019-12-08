package aggregates

import (
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
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

func (agg *Distinct) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text(fmt.Sprintf("Works like [%s](#%s), only taking into account distinct elements per group.", agg.underlying.String(), agg.underlying.String()))),
		),
	)
}

func (agg *Distinct) AddRecord(key octosql.Value, value octosql.Value) error {
	groupSet, previousValueExists, err := agg.groupSets.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get distinct value set for group key")
	}

	var newGroupSet *execution.HashMap
	if !previousValueExists {
		newGroupSet = execution.NewHashMap()
	} else {
		newGroupSet = groupSet.(*execution.HashMap)
	}

	_, previousValueExists, err = newGroupSet.Get(value)
	if err != nil {
		return errors.Wrap(err, "couldn't get value out of distinct value set for given key")
	}

	if previousValueExists {
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

func (agg *Distinct) GetAggregated(key octosql.Value) (octosql.Value, error) {
	return agg.underlying.GetAggregated(key)
}

func (agg *Distinct) String() string {
	return fmt.Sprintf("%s_distinct", agg.underlying.String())
}

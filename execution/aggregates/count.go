package aggregates

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
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

func (agg *Count) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Averages elements in the group.")),
		),
	)
}

func (agg *Count) AddRecord(key octosql.Value, value octosql.Value) error {
	count, previousValueExists, err := agg.counts.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current count out of hashmap")
	}

	var newCount int
	if previousValueExists {
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

func (agg *Count) GetAggregated(key octosql.Value) (octosql.Value, error) {
	count, ok, err := agg.counts.Get(key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get count out of hashmap")
	}

	if !ok {
		return octosql.ZeroValue(), errors.Errorf("count for key not found")
	}

	return octosql.MakeInt(count.(int)), nil
}

func (agg *Count) String() string {
	return "count"
}

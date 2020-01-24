package aggregates

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
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

func (agg *Last) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Takes the last received element in the group.")),
		),
	)
}

func (agg *Last) AddRecord(key octosql.Value, value octosql.Value) error {
	err := agg.lasts.Set(key, value)
	if err != nil {
		return errors.Wrap(err, "couldn't put new last into hashmap")
	}

	return nil
}

func (agg *Last) GetAggregated(key octosql.Value) (octosql.Value, error) {
	last, ok, err := agg.lasts.Get(key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get last out of hashmap")
	}

	if !ok {
		return octosql.ZeroValue(), errors.Errorf("last for key not found")
	}

	return last.(octosql.Value), nil
}

func (agg *Last) String() string {
	return "last"
}

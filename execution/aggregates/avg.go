package aggregates

import (
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Average struct {
	averages   *execution.HashMap
	counts     *execution.HashMap
	typedValue octosql.Value
}

func NewAverage() *Average {
	return &Average{
		averages: execution.NewHashMap(),
		counts:   execution.NewHashMap(),
	}
}

func (agg *Average) Document() docs.Documentation {
	return docs.Section(
		agg.String(),
		docs.Body(
			docs.Section("Description", docs.Text("Averages Floats, Ints or Durations in the group. You may not mix types.")),
		),
	)
}

func (agg *Average) AddRecord(key octosql.Value, value octosql.Value) error {
	if octosql.AreEqual(agg.typedValue, octosql.ZeroValue()) {
		agg.typedValue = value
	}

	if agg.typedValue.GetType() != value.GetType() {
		return errors.Errorf("mixed types in avg: %v and %v with values %v and %v",
			value.GetType(), agg.typedValue.GetType(),
			value, agg.typedValue)
	}

	var floatValue float64
	switch value.GetType() {
	case octosql.TypeFloat:
		floatValue = value.AsFloat()
	case octosql.TypeInt:
		floatValue = float64(value.AsInt())
	case octosql.TypeDuration:
		floatValue = float64(value.AsDuration())
	default:
		return errors.Errorf("invalid type in average: %v with value %v", value.GetType(), value)
	}

	count, previousValueExists, err := agg.counts.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current element count out of hashmap")
	}

	average, _, err := agg.averages.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get current average out of hashmap")
	}

	var newAverage float64
	var newCount int
	if previousValueExists {
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

func (agg *Average) GetAggregated(key octosql.Value) (octosql.Value, error) {
	average, ok, err := agg.averages.Get(key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get average out of hashmap")
	}

	if !ok {
		return octosql.ZeroValue(), errors.Errorf("average for key not found")
	}

	switch agg.typedValue.GetType() {
	case octosql.TypeDuration:
		return octosql.MakeDuration(time.Duration(average.(float64))), nil
	default:
		return octosql.MakeFloat(average.(float64)), nil
	}
}

func (agg *Average) String() string {
	return "avg"
}

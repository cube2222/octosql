package execution

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
)

type AggregateMock struct {
	addI      int
	addKeys   []octosql.Tuple
	addValues []octosql.Value

	getI      int
	getKeySet map[octosql.Value]struct{}
	getValues []octosql.Value

	t *testing.T
}

func (mock *AggregateMock) AddRecord(key octosql.Tuple, value octosql.Value) error {
	if !reflect.DeepEqual(mock.addKeys[mock.addI], key) {
		mock.t.Errorf("invalid %v call key: got %v wanted %v", mock.addI, key, mock.addKeys[mock.addI])
	}
	if !reflect.DeepEqual(mock.addValues[mock.addI], value) {
		mock.t.Errorf("invalid %v call value: got %v wanted %v", mock.addI, value, mock.addValues[mock.addI])
	}
	mock.addI++
	return nil
}

func (mock *AggregateMock) GetAggregated(key octosql.Tuple) (octosql.Value, error) {
	_, ok := mock.getKeySet[key[0]]
	if !ok {
		mock.t.Errorf("invalid %v call key: got %v wanted one of %v", mock.getI, key, mock.getKeySet)
	}
	delete(mock.getKeySet, key[0])
	mock.getI++
	return mock.getValues[mock.getI-1], nil
}

func (*AggregateMock) String() string {
	return "mock"
}

func TestGroupBy_AggregateCalling(t *testing.T) {
	fields := []octosql.VariableName{"cat", "livesleft", "ownerid"}

	firstAggregate := &AggregateMock{
		addKeys: []octosql.Tuple{
			{octosql.MakeInt(5)},
			{octosql.MakeInt(4)},
			{octosql.MakeInt(3)},
			{octosql.MakeInt(3)},
			{octosql.MakeInt(3)},
		},
		addValues: []octosql.Value{
			octosql.MakeString("Buster"),
			octosql.MakeString("Precious"),
			octosql.MakeString("Nala"),
			octosql.MakeString("Tiger"),
			octosql.MakeString("Lucy"),
		},

		getKeySet: map[octosql.Value]struct{}{
			octosql.MakeInt(5): {},
			octosql.MakeInt(4): {},
			octosql.MakeInt(3): {},
		},
		getValues: []octosql.Value{
			octosql.MakeString("Buster"),
			octosql.MakeString("Precious"),
			octosql.MakeString("Nala"),
		},

		t: t,
	}
	secondAggregate := &AggregateMock{
		addKeys: []octosql.Tuple{
			{octosql.MakeInt(5)},
			{octosql.MakeInt(4)},
			{octosql.MakeInt(3)},
			{octosql.MakeInt(3)},
			{octosql.MakeInt(3)},
		},
		addValues: []octosql.Value{
			octosql.MakeInt(9),
			octosql.MakeInt(6),
			octosql.MakeInt(5),
			octosql.MakeInt(4),
			octosql.MakeInt(3),
		},

		getKeySet: map[octosql.Value]struct{}{
			octosql.MakeInt(5): {},
			octosql.MakeInt(4): {},
			octosql.MakeInt(3): {},
		},
		getValues: []octosql.Value{
			octosql.MakeInt(9),
			octosql.MakeInt(6),
			octosql.MakeInt(4),
		},

		t: t,
	}

	groupby := &GroupByStream{
		source: NewInMemoryStream([]*Record{
			NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5}),
			NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
			NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 5, 3}),
			NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 4, 3}),
			NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 3, 3}),
		}),
		variables:  octosql.NoVariables(),
		key:        []Expression{NewVariable("ownerid")},
		groups:     NewHashMap(),
		fields:     []octosql.VariableName{"cat", "livesleft"},
		aggregates: []Aggregate{firstAggregate, secondAggregate},
		as:         []octosql.VariableName{"", "lives_left"},
	}

	outFields := []octosql.VariableName{"cat_mock", "lives_left"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"Buster", 9}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"Precious", 6}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"Nala", 4}),
	}

	var rec *Record
	var err error
	i := 0
	for rec, err = groupby.Next(); err == nil; rec, err = groupby.Next() {
		if !reflect.DeepEqual(rec, expectedOutput[i]) {
			t.Errorf("Record got %+v wanted %+v", rec, expectedOutput[i])
			return
		}
		i++
	}
	if err != ErrEndOfStream {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if firstAggregate.addI != len(firstAggregate.addKeys) {
		t.Errorf("invalid firstAggregate add call count: got %v wanted %v", firstAggregate.addI, len(firstAggregate.addKeys))
	}
	if len(firstAggregate.getKeySet) != 0 {
		t.Errorf("invalid firstAggregate get call count: still waiting for %v", firstAggregate.getKeySet)
	}

	if secondAggregate.addI != len(secondAggregate.addKeys) {
		t.Errorf("invalid secondAggregate add call count: got %v wanted %v", secondAggregate.addI, len(secondAggregate.addKeys))
	}
	if len(secondAggregate.getKeySet) != 0 {
		t.Errorf("invalid secondAggregate get call count: still waiting for %v", secondAggregate.getKeySet)
	}
}

package aggregates

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

type AddRecordMock struct {
	i      int
	keys   []octosql.Tuple
	values []octosql.Value

	t *testing.T
}

func (mock *AddRecordMock) AddRecord(key octosql.Tuple, value octosql.Value) error {
	if !reflect.DeepEqual(mock.keys[mock.i], key) {
		mock.t.Errorf("invalid %v call key: got %v wanted %v", mock.i, key, mock.keys[mock.i])
	}
	if !reflect.DeepEqual(mock.values[mock.i], value) {
		mock.t.Errorf("invalid %v call value: got %v wanted %v", mock.i, value, mock.values[mock.i])
	}
	mock.i++
	return nil
}

func (*AddRecordMock) GetAggregated(key octosql.Tuple) (octosql.Value, error) {
	panic("implement me")
}

func (*AddRecordMock) String() string {
	panic("implement me")
}

func TestDistinct_AddRecord(t *testing.T) {
	outerKeys := []octosql.Tuple{
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
	}

	outerValues := []octosql.Value{
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value2"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value2"), octosql.MakeInt(3)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(3)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
	}

	distinctKeys := []octosql.Tuple{
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
		{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})},
	}

	distinctValues := []octosql.Value{
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value2"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value2"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value2"), octosql.MakeInt(3)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(2)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
		octosql.MakeTuple([]octosql.Value{octosql.MakeString("value"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("value3"), octosql.MakeInt(3)}), octosql.MakeObject(map[string]octosql.Value{"value": octosql.MakeInt(1)})}),
	}

	mock := &AddRecordMock{
		keys:   distinctKeys,
		values: distinctValues,
		t:      t,
	}

	distinct := &Distinct{
		underlying: mock,
		groupSets:  execution.NewHashMap(),
	}

	for i := range outerKeys {
		err := distinct.AddRecord(outerKeys[i], outerValues[i])
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	if mock.i != len(distinctKeys) {
		t.Errorf("invalid call count: got %v wanted %v", mock.i, len(distinctKeys))
	}
}

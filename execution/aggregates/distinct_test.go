package aggregates

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

type AddRecordMock struct {
	i      int
	keys   [][]interface{}
	values []interface{}

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
	outerKeys := [][]interface{}{
		{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key2", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key2", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key2", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
	}

	outerValues := []interface{}{
		[]interface{}{"value", 1, []interface{}{"value", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value2", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value2", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value2", 3}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 3}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 1}, map[string]interface{}{"value": 1}},
	}

	distinctKeys := [][]interface{}{
		{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key2", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key2", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key2", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
		{"key", 1, []interface{}{"key3", 1}, map[string]interface{}{"key": 1}},
	}

	distinctValues := []interface{}{
		[]interface{}{"value", 1, []interface{}{"value", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value2", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value2", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value2", 3}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 1}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 2}, map[string]interface{}{"value": 1}},
		[]interface{}{"value", 1, []interface{}{"value3", 3}, map[string]interface{}{"value": 1}},
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

package aggregates

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/execution"
)

func TestFirst(t *testing.T) {
	type kv struct {
		key   []interface{}
		value interface{}
	}
	tests := []struct {
		name    string
		args    []kv
		key     []interface{}
		want    interface{}
		wantErr bool
	}{
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key"},
					value: 5,
				},
			},
			key:  []interface{}{"key"},
			want: 5,
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 6,
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: 4,
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 6,
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 7,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: 4,
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 7,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 6,
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key"},
					value: map[string]interface{}{"test": 1},
				},
			},
			key:  []interface{}{"key"},
			want: map[string]interface{}{"test": 1},
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: []interface{}{1, 2, []interface{}{"test", 5.0}},
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: []interface{}{1, 2, []interface{}{"test", 5.0}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &First{
				firsts: execution.NewHashMap(),
			}
			for i := range tt.args {
				if err := agg.AddRecord(tt.args[i].key, tt.args[i].value); err != nil {
					if !tt.wantErr {
						t.Errorf("First.AddRecord() error = %v", err)
					}
					return
				}
			}

			got, err := agg.GetAggregated(tt.key)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("First.GetAggregated() error = %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("First: wanted error")
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("First.GetAggregated() = %v, want %v", got, tt.want)
			}
		})
	}
}

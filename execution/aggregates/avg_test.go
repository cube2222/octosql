package aggregates

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/execution"
)

func TestAverage(t *testing.T) {
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
			want: 5.0,
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
			want: 6.0,
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
			want: 4.0,
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
			want: 6.0,
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
			want: 4.0,
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
			want: 7.5,
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key"},
					value: 5.0,
				},
			},
			key:  []interface{}{"key"},
			want: 5.0,
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 6.0,
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: 4.0,
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 6.0,
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 7.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: 4.0,
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 7.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 7.5,
		},
		{
			name: "type error",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 7,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9.0,
				},
			},
			key:     []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			wantErr: true,
		},
		{
			name: "type error",
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
					value: 7.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9.0,
				},
			},
			key:     []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			wantErr: true,
		},
		{
			name: "type error",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 4,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 7.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 8.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 9.0,
				},
			},
			key:     []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &Average{
				averages: execution.NewHashMap(),
				counts:   execution.NewHashMap(),
			}
			for i := range tt.args {
				if err := agg.AddRecord(tt.args[i].key, tt.args[i].value); err != nil {
					if !tt.wantErr {
						t.Errorf("Average.AddRecord() error = %v", err)
					}
					return
				}
			}

			got, err := agg.GetAggregated(tt.key)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Average.GetAggregated() error = %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("Average: wanted error")
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Average.GetAggregated() = %v, want %v", got, tt.want)
			}
		})
	}
}

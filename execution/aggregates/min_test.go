package aggregates

import (
	"reflect"
	"testing"
	"time"

	"github.com/cube2222/octosql/execution"
)

func TestMin(t *testing.T) {
	now := time.Now().Add(time.Hour - 1)

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
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 5,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 5,
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 5,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 5,
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
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 7,
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
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 5,
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
					value: 5.0,
				},
			},
			key:  []interface{}{"key"},
			want: 5.0,
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 5.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 5.0,
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 5.0,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: 6.0,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: 5.0,
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
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 7.0,
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
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: 5.0,
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
			want: 6.0,
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key"},
					value: "aab",
				},
			},
			key:  []interface{}{"key"},
			want: "aab",
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "aaa",
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: "aaa",
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "aaa",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abb",
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: "aaa",
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: "aaa",
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: "aaa",
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: "aaa",
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: "abb",
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "aaa",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: "aaa",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "aba",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abc",
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: "aaa",
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "aaa",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: "aaa",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "aba",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abb",
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: "abc",
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: "aaa",
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   []interface{}{"key"},
					value: now,
				},
			},
			key:  []interface{}{"key"},
			want: now,
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: now,
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour),
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: now,
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: now,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: now,
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: now,
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: now.Add(time.Hour),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: now,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: now.Add(time.Hour * 2),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour * 2),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour * 3),
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
			want: now,
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: now,
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key1": 1}},
					value: now.Add(time.Hour * 2),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour * 2),
				},
				{
					key:   []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
					value: now.Add(time.Hour * 3),
				},
			},
			key:  []interface{}{"key", 1, []interface{}{"key", 1}, map[string]interface{}{"key": 1}},
			want: now,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &Min{
				mins: execution.NewHashMap(),
			}
			for i := range tt.args {
				if err := agg.AddRecord(tt.args[i].key, tt.args[i].value); err != nil {
					if !tt.wantErr {
						t.Errorf("Min.AddRecord() error = %v", err)
					}
					return
				}
			}

			got, err := agg.GetAggregated(tt.key)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Min.GetAggregated() error = %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("Min: wanted error")
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Min.GetAggregated() = %v, want %v", got, tt.want)
			}
		})
	}
}

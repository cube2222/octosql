package aggregates

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func TestLast(t *testing.T) {
	type kv struct {
		key   octosql.Tuple
		value octosql.Value
	}
	tests := []struct {
		name    string
		args    []kv
		key     octosql.Tuple
		want    octosql.Value
		wantErr bool
	}{
		{
			name: "one element",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
					value: octosql.MakeInt(5),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
			want: octosql.MakeInt(5),
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(6),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeInt(6),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(6),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(4),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeInt(4),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(6),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(4),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeInt(6),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(6),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(4),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(7),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(8),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(9),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeInt(4),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(6),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(4),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(7),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(8),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(9),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeInt(9),
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
					value: octosql.MakeObject(map[string]octosql.Value{"test": octosql.MakeInt(1)}),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
			want: octosql.MakeObject(map[string]octosql.Value{"test": octosql.MakeInt(1)}),
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTuple([]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeTuple([]octosql.Value{octosql.MakeString("test"), octosql.MakeFloat(5.0)})}),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeTuple([]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeTuple([]octosql.Value{octosql.MakeString("test"), octosql.MakeFloat(5.0)})}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &Last{
				lasts: execution.NewHashMap(),
			}
			for i := range tt.args {
				if err := agg.AddRecord(tt.args[i].key, tt.args[i].value); err != nil {
					if !tt.wantErr {
						t.Errorf("Last.AddRecord() error = %v", err)
					}
					return
				}
			}

			got, err := agg.GetAggregated(tt.key)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Last.GetAggregated() error = %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("Last: wanted error")
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Last.GetAggregated() = %v, want %v", got, tt.want)
			}
		})
	}
}

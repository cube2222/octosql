package aggregates

import (
	"reflect"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func TestMax(t *testing.T) {
	now := time.Now().Add(time.Hour - 1)

	type kv struct {
		key   octosql.Value
		value octosql.Value
	}
	tests := []struct {
		name    string
		args    []kv
		key     octosql.Value
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
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(6),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(5),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeInt(6),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(5),
				},
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
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(7),
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
			want: octosql.MakeInt(7),
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
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeInt(5),
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
					value: octosql.MakeFloat(5.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
			want: octosql.MakeFloat(5.0),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(6.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(5.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeFloat(6.0),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(5.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(6.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeFloat(6.0),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(6.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(4.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeFloat(4.0),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(6.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(4.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeFloat(6.0),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(6.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(4.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(7.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(7.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(8.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(9.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeFloat(7.0),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(6.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(4.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(7.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(5.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(8.0),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeFloat(9.0),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeFloat(9.0),
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
					value: octosql.MakeString("aab"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
			want: octosql.MakeString("aab"),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeString("abb"),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeString("abb"),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeString("aaa"),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeString("abb"),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aba"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abc"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeString("abb"),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aaa"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("aba"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abb"),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeString("abc"),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeString("abc"),
		},
		{
			name: "one element",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
					value: octosql.MakeTime(now),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key")}),
			want: octosql.MakeTime(now),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeTime(now.Add(time.Hour)),
		},
		{
			name: "two elements",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour)),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeTime(now.Add(time.Hour)),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeTime(now),
		},
		{
			name: "many single-element groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeTime(now.Add(time.Hour)),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour * 2)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour * 2)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour * 3)),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
			want: octosql.MakeTime(now.Add(time.Hour * 2)),
		},
		{
			name: "many groups",
			args: []kv{
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key1": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour * 2)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour * 2)),
				},
				{
					key:   octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
					value: octosql.MakeTime(now.Add(time.Hour * 3)),
				},
			},
			key:  octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1), octosql.MakeTuple([]octosql.Value{octosql.MakeString("key"), octosql.MakeInt(1)}), octosql.MakeObject(map[string]octosql.Value{"key": octosql.MakeInt(1)})}),
			want: octosql.MakeTime(now.Add(time.Hour * 3)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &Max{
				maxes: execution.NewHashMap(),
			}
			for i := range tt.args {
				if err := agg.AddRecord(tt.args[i].key, tt.args[i].value); err != nil {
					if !tt.wantErr {
						t.Errorf("Max.AddRecord() error = %v", err)
					}
					return
				}
			}

			got, err := agg.GetAggregated(tt.key)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Max.GetAggregated() error = %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("Max: wanted error")
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Max.GetAggregated() = %v, want %v", got, tt.want)
			}
		})
	}
}

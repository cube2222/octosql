package streaming

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
)

func TestMarshal(t *testing.T) {
	type args struct {
		v octosql.Value
		b octosql.Value
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "string test",
			args: args{
				v: octosql.MakeString("ala ma kota i psa"),
				b: octosql.ZeroString(),
			},
		},
		{
			name: "negative int test",
			args: args{
				v: octosql.MakeInt(-18249),
				b: octosql.ZeroInt(),
			},
		},
		{
			name: "positive int test",
			args: args{
				v: octosql.MakeInt(1587129),
				b: octosql.ZeroInt(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytes, err := SortedMarshal(&tt.args.v)
			if err != nil {
				t.Errorf("SortedMarshal() error = %v, wantErr false", err)
				return
			}

			err = SortedUnmarshal(bytes, &tt.args.b)
			if err != nil {
				t.Errorf("SortedMarshal() error = %v, wantErr false", err)
				return
			}

			if !reflect.DeepEqual(tt.args.v, tt.args.b) {
				t.Errorf("Values are not equal!")
			}
		})
	}
}

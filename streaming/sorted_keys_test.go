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

func TestSortedMarshalString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SortedMarshalString(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("SortedMarshalString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SortedMarshalString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortedMarshalInt(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SortedMarshalInt(tt.args.i)
			if (err != nil) != tt.wantErr {
				t.Errorf("SortedMarshalInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SortedMarshalInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

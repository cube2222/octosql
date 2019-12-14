package streaming

import (
	"math"
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
			bytes := SortedMarshal(&tt.args.v)

			err := SortedUnmarshal(bytes, &tt.args.b)
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

func TestMarshalContinuity(t *testing.T) {
	type args struct {
		values []octosql.Value
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "strings test",
			args: args{
				values: []octosql.Value{
					octosql.MakeString("ala ma kota"),
					octosql.MakeString("ala ma psa"),
					octosql.MakeString("bartek ma papugi"),
				},
			},
		},
		{
			name: "ints test",
			args: args{
				values: []octosql.Value{
					octosql.MakeInt(math.MinInt64),
					octosql.MakeInt(-456),
					octosql.MakeInt(-189),
					octosql.MakeInt(-10),
					octosql.MakeInt(0),
					octosql.MakeInt(1),
					octosql.MakeInt(3),
					octosql.MakeInt(17),
					octosql.MakeInt(24287),
					octosql.MakeInt(math.MaxInt64),
				},
			},
		},
		{
			name: "floats test",
			args: args{
				values: []octosql.Value{
					// TODO - nie dziala dla ujemnych :<
					//octosql.MakeFloat(-124.0001),
					//octosql.MakeFloat(-123.9998),
					//octosql.MakeFloat(-1.01),
					octosql.MakeFloat(0.0000001),
					octosql.MakeFloat(1.01),
					octosql.MakeFloat(3),
					octosql.MakeFloat(2345.5432),
					octosql.MakeFloat(24287.111111),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			argCount := len(tt.args.values)
			marshaled := make([][]byte, argCount)

			for i := 0; i < argCount; i++ {
				bytes := SortedMarshal(&tt.args.values[i])
				marshaled[i] = bytes
			}

			if !isIncreasing(marshaled) {
				t.Errorf("The marshaled values aren't increasing!")
			}
		})
	}
}

/* Auxiliary functions */
func isIncreasing(b [][]byte) bool {
	for i := 0; i < len(b)-1; i++ {
		if compareByteSlices(b[i], b[i+1]) != -1 {
			return false
		}
	}

	return true
}

func compareByteSlices(b1, b2 []byte) int {
	b1Length := len(b1)
	b2Length := len(b2)
	length := intMin(b1Length, b2Length)

	for i := 0; i < length; i++ {
		if b1[i] < b2[i] {
			return -1
		} else if b1[i] > b2[i] {
			return 1
		}
	}

	if b1Length > b2Length {
		return 1
	} else if b2Length == b1Length {
		return 0
	} else {
		return -1
	}
}

func intMin(x, y int) int {
	if x >= y {
		return y
	}

	return x
}

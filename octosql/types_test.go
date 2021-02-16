package octosql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTypeIntersection(t *testing.T) {
	some := func(t Type) *Type {
		return &t
	}

	tests := []struct {
		t1   Type
		t2   Type
		want *Type
	}{
		{
			t1:   String,
			t2:   String,
			want: some(String),
		},
		{
			t1:   Int,
			t2:   String,
			want: nil,
		},
		{
			t1:   TypeSum(Boolean, Time),
			t2:   TypeSum(Time, Int),
			want: some(Time),
		},
		{
			t1:   TypeSum(TypeSum(Boolean, Time), String),
			t2:   TypeSum(TypeSum(Time, Int), String),
			want: some(TypeSum(String, Time)),
		},
		{
			t1:   TypeSum(Boolean, Time),
			t2:   TypeSum(String, Int),
			want: nil,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if got := TypeIntersection(tt.t1, tt.t2); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TypeIntersection(%s, %s) = %s, want %s", tt.t1, tt.t2, got, tt.want)
			}
		})
	}
}

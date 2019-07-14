package execution

import (
	"reflect"
	"testing"
	"time"

	"github.com/cube2222/octosql"
)

func TestParseType(t *testing.T) {
	tests := []struct {
		name string
		str  string
		want octosql.Value
	}{
		{
			name: "parse int",
			str:  "123",
			want: octosql.MakeInt(123),
		},
		{
			name: "parse int",
			str:  "-123",
			want: octosql.MakeInt(-123),
		},
		{
			name: "parse boolean",
			str:  "true",
			want: octosql.MakeBool(true),
		},
		{
			name: "parse boolean",
			str:  "TRUE",
			want: octosql.MakeBool(true),
		},
		{
			name: "parse boolean",
			str:  "True",
			want: octosql.MakeBool(true),
		},
		{
			name: "parse float",
			str:  "123.0",
			want: octosql.MakeFloat(123.0),
		},
		{
			name: "parse float",
			str:  "-123.0",
			want: octosql.MakeFloat(-123.0),
		},
		{
			name: "parse float",
			str:  "123.123",
			want: octosql.MakeFloat(123.123),
		},
		{
			name: "parse date",
			str:  "2019-03-17T15:44:16+00:00",
			want: octosql.MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
		},
		{
			name: "parse date",
			str:  "2019-03-17T15:44:16+01:00",
			want: octosql.MakeTime(time.Date(2019, 03, 17, 14, 44, 16, 0, time.UTC)),
		},
		{
			name: "parse json",
			str:  `{"name": "Jakub", "age": 3, "city": {"name": "warsaw", "population": 1700000}, "array": ["value1", "value2"]}`,
			want: octosql.MakeObject(map[string]octosql.Value{
				"name": octosql.MakeString("Jakub"),
				"age":  octosql.MakeFloat(3.0),
				"city": octosql.MakeObject(map[string]octosql.Value{
					"name":       octosql.MakeString("warsaw"),
					"population": octosql.MakeFloat(1700000.0),
				}),
				"array": octosql.Tuple{
					octosql.MakeString("value1"),
					octosql.MakeString("value2"),
				},
			}),
		},
		{
			name: "unparsable",
			str:  "Lorem ipsum.",
			want: octosql.MakeString("Lorem ipsum."),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseType(tt.str)
			if reflect.TypeOf(got) != reflect.TypeOf(tt.want) {
				t.Fatalf("ParseType() = type %v, want type %v",
					reflect.TypeOf(got), reflect.TypeOf(tt.want))
			}
			if wantTime, ok := tt.want.(octosql.Time); ok {
				if !wantTime.AsTime().Equal(got.(octosql.Time).AsTime()) {
					t.Fatalf("ParseType() = %+v, want %+v", got, tt.want)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseType() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

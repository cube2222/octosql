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
				if !wantTime.Time().Equal(got.(octosql.Time).Time()) {
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

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  octosql.Value
	}{
		{
			name: "normalize something complex",
			value: map[string]interface{}{
				"name": []byte("Jakub"),
				"age":  uint8(3),
				"city": map[string]interface{}{
					"name":       "warsaw",
					"population": float32(1700000),
				},
				"array": []interface{}{[]interface{}{float32(1), uint8(2), int64(3)}, true},
			},
			want: octosql.MakeObject(map[string]octosql.Value{
				"name": octosql.MakeString("Jakub"),
				"age":  octosql.MakeInt(3),
				"city": octosql.MakeObject(map[string]octosql.Value{
					"name":       octosql.MakeString("warsaw"),
					"population": octosql.MakeFloat(1700000.0),
				}),
				"array": octosql.Tuple{
					octosql.Tuple{
						octosql.MakeFloat(1),
						octosql.MakeInt(2),
						octosql.MakeInt(3),
					},
					octosql.MakeBool(true),
				},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeType(tt.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NormalizeType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAreEqual(t *testing.T) {
	type args struct {
		left  octosql.Value
		right octosql.Value
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "compare ints",
			args: args{
				left:  octosql.MakeInt(1),
				right: octosql.MakeInt(1),
			},
			want: true,
		},
		{
			name: "compare ints",
			args: args{
				left:  octosql.MakeInt(1),
				right: octosql.MakeInt(2),
			},
			want: false,
		},
		{
			name: "compare times",
			args: args{
				left:  octosql.MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
				right: octosql.MakeTime(time.Date(2019, 03, 17, 20, 44, 16, 0, time.FixedZone("anything", 3600*5))),
			},
			want: true,
		},
		{
			name: "compare records",
			args: args{
				left: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					[]interface{}{
						15,
						"something",
						[]interface{}{1, 2, 3},
					},
				),
				right: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					[]interface{}{
						15,
						"something",
						[]interface{}{1, 2, 3},
					},
				),
			},
			want: true,
		},
		{
			name: "records compare positionally",
			args: args{
				left: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
					},
					[]interface{}{
						15,
						"something",
					},
				),
				right: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					[]interface{}{
						15,
						"something",
					},
				),
			},
			want: true,
		},
		{
			name: "unequal records",
			args: args{
				left: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
					},
					[]interface{}{
						15,
						"something",
					},
				),
				right: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					[]interface{}{
						12,
						"something_else",
					},
				),
			},
			want: false,
		},
		{
			name: "unequal records",
			args: args{
				left: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
					},
					[]interface{}{
						15,
					},
				),
				right: NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
					},
					[]interface{}{
						15,
						"something",
					},
				),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AreEqual(tt.args.left, tt.args.right); got != tt.want {
				t.Errorf("AreEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

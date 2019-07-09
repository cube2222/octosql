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
		want interface{}
	}{
		{
			name: "parse int",
			str:  "123",
			want: 123,
		},
		{
			name: "parse int",
			str:  "-123",
			want: -123,
		},
		{
			name: "parse boolean",
			str:  "true",
			want: true,
		},
		{
			name: "parse boolean",
			str:  "TRUE",
			want: true,
		},
		{
			name: "parse boolean",
			str:  "True",
			want: true,
		},
		{
			name: "parse float",
			str:  "123.0",
			want: 123.0,
		},
		{
			name: "parse float",
			str:  "123.0",
			want: 123.0,
		},
		{
			name: "parse float",
			str:  "123.123",
			want: 123.123,
		},
		{
			name: "parse date",
			str:  "2019-03-17T15:44:16+00:00",
			want: time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC),
		},
		{
			name: "parse date",
			str:  "2019-03-17T15:44:16+01:00",
			want: time.Date(2019, 03, 17, 14, 44, 16, 0, time.UTC),
		},
		{
			name: "parse json",
			str:  `{"name": "Jakub", "age": 3, "city": {"name": "warsaw", "population": 1700000}, "array": ["value1", "value2"]}`,
			want: map[string]interface{}{
				"name": "Jakub",
				"age":  3.0,
				"city": map[string]interface{}{
					"name":       "warsaw",
					"population": 1700000.0,
				},
				"array": octosql.Tuple{"value1", "value2"},
			},
		},
		{
			name: "unparsable",
			str:  "Lorem ipsum.",
			want: "Lorem ipsum.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseType(tt.str)
			if reflect.TypeOf(got) != reflect.TypeOf(tt.want) {
				t.Fatalf("ParseType() = type %v, want type %v",
					reflect.TypeOf(got), reflect.TypeOf(tt.want))
			}
			if wantTime, ok := tt.want.(time.Time); ok {
				if !wantTime.Equal(got.(time.Time)) {
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
		want  interface{}
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
				"array": octosql.Tuple{octosql.Tuple{float32(1), uint8(2), int64(3)}, true},
			},
			want: map[string]interface{}{
				"name": "Jakub",
				"age":  int(3),
				"city": map[string]interface{}{
					"name":       "warsaw",
					"population": float64(1700000.0),
				},
				"array": octosql.Tuple{octosql.Tuple{float64(1), int(2), int(3)}, true},
			},
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
		left  interface{}
		right interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "compare ints",
			args: args{
				left:  1,
				right: 1,
			},
			want: true,
		},
		{
			name: "compare ints",
			args: args{
				left:  1,
				right: 2,
			},
			want: false,
		},
		{
			name: "compare times",
			args: args{
				left:  time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC),
				right: time.Date(2019, 03, 17, 20, 44, 16, 0, time.FixedZone("anything", 3600*5)),
			},
			want: true,
		},
		{
			name: "compare records",
			args: args{
				left: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("a"): 15,
						octosql.NewVariableName("b"): "something",
						octosql.NewVariableName("c"): octosql.Tuple{1, 2, 3},
					},
				),
				right: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("a"): 15,
						octosql.NewVariableName("b"): "something",
						octosql.NewVariableName("c"): octosql.Tuple{1, 2, 3},
					},
				),
			},
			want: true,
		},
		{
			name: "records compare positionally",
			args: args{
				left: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("a"): 15,
						octosql.NewVariableName("b"): "something",
					},
				),
				right: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("b"): 15,
						octosql.NewVariableName("c"): "something",
					},
				),
			},
			want: true,
		},
		{
			name: "unequal records",
			args: args{
				left: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("a"): 15,
						octosql.NewVariableName("b"): "something",
					},
				),
				right: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("b"),
						octosql.NewVariableName("c"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("b"): 12,
						octosql.NewVariableName("c"): "something_else",
					},
				),
			},
			want: false,
		},
		{
			name: "unequal records",
			args: args{
				left: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("a"): 15,
					},
				),
				right: NewRecord(
					[]octosql.VariableName{
						octosql.NewVariableName("a"),
						octosql.NewVariableName("b"),
					},
					map[octosql.VariableName]interface{}{
						octosql.NewVariableName("a"): 15,
						octosql.NewVariableName("b"): "something",
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

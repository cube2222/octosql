package octosql

import (
	"reflect"
	"testing"
	"time"
)

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  Value
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
			want: MakeObject(map[string]Value{
				"name": MakeString("Jakub"),
				"age":  MakeInt(3),
				"city": MakeObject(map[string]Value{
					"name":       MakeString("warsaw"),
					"population": MakeFloat(1700000.0),
				}),
				"array": Tuple{
					Tuple{
						MakeFloat(1),
						MakeInt(2),
						MakeInt(3),
					},
					MakeBool(true),
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
		left  Value
		right Value
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "compare ints",
			args: args{
				left:  MakeInt(1),
				right: MakeInt(1),
			},
			want: true,
		},
		{
			name: "compare ints",
			args: args{
				left:  MakeInt(1),
				right: MakeInt(2),
			},
			want: false,
		},
		{
			name: "compare times",
			args: args{
				left:  MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
				right: MakeTime(time.Date(2019, 03, 17, 20, 44, 16, 0, time.FixedZone("anything", 3600*5))),
			},
			want: true,
		},
		{
			name: "compare tuples",
			args: args{
				left: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
						MakeTuple(
							[]Value{
								MakeInt(1),
								MakeInt(2),
								MakeInt(3),
							},
						),
					},
				),
				right: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
						MakeTuple(
							[]Value{
								MakeInt(1),
								MakeInt(2),
								MakeInt(3),
							},
						),
					},
				),
			},
			want: true,
		},
		{
			name: "unequal tuples",
			args: args{
				left: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
					},
				),
				right: MakeTuple(
					[]Value{
						MakeInt(12),
						MakeString("something else"),
					},
				),
			},
			want: false,
		},
		{
			name: "unequal records",
			args: args{
				left: MakeTuple(
					[]Value{
						MakeInt(15),
					},
				),
				right: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
					},
				),
			},
			want: false,
		},
		{
			name: "compare nil to non-nil",
			args: args{
				left:  nil,
				right: MakeInt(7),
			},
			want: false,
		},
		{
			name: "compare non-nil to nil",
			args: args{
				left:  MakeInt(3),
				right: nil,
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

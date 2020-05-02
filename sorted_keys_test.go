package octosql

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestMarshal(t *testing.T) {
	type args struct {
		v Value
	}
	tests := []struct {
		name string
		args args
	}{
		/* null test */
		{
			name: "null test",
			args: args{
				v: MakeNull(),
			},
		},
		/* phantom test */
		{
			name: "phantom test",
			args: args{
				v: MakePhantom(),
			},
		},

		/* int tests */
		{
			name: "int test with a negative number",
			args: args{
				v: MakeInt(-18249),
			},
		},
		{
			name: "int test with a positive number",
			args: args{
				v: MakeInt(1587129),
			},
		},
		{
			name: "int test with a zero",
			args: args{
				v: MakeInt(0),
			},
		},
		/* float tests */
		{
			name: "float test - positive float",
			args: args{
				v: MakeFloat(1827.128852),
			},
		},
		{
			name: "float test - negative float",
			args: args{
				v: MakeFloat(-192.11239),
			},
		},
		/* bool tests */
		{
			name: "bool test - false",
			args: args{
				v: MakeBool(false),
			},
		},
		{
			name: "bool test - true",
			args: args{
				v: MakeBool(true),
			},
		},
		/* string tests */
		{
			name: "string test",
			args: args{
				v: MakeString("ala ma kota i psa"),
			},
		},
		{
			name: "string test with weird values",
			args: args{
				v: MakeString(string([]byte{28, 192, 0, 123, 11, 99, 243, 172, 111, 3, 4, 5})),
			},
		},
		/* timestamp tests */
		{
			name: "time test - now",
			args: args{
				v: MakeTime(time.Now()),
			},
		},
		{
			name: "time test - fixed date",
			args: args{
				v: MakeTime(time.Date(2019, 19, 12, 18, 05, 14, 28183, time.UTC)),
			},
		},
		/* duration tests */
		{
			name: "duration test - some positive value",
			args: args{
				v: MakeDuration(1888),
			},
		},
		{
			name: "duration test - zero",
			args: args{
				v: MakeDuration(0),
			},
		},
		/* tuple tests */
		{
			name: "tuple test - basic types",
			args: args{
				v: MakeTuple([]Value{MakeString("ala"), MakeInt(17), MakeInt(99)}),
			},
		},
		{
			name: "tuple test - multiple strings inside",
			args: args{
				v: MakeTuple([]Value{MakeString("ala"), MakeString("ma"), MakeInt(1823792), MakeString("kotów")}),
			},
		},
		{
			name: "tuple test - tuple inside a tuple",
			args: args{
				v: MakeTuple([]Value{
					MakeString("a"),
					MakeTuple([]Value{
						MakeString("b"),
						MakeString("c"),
						MakeInt(TupleDelimiter),
						MakeInt(StringDelimiter),
						MakeString("d"),
					}),
					MakeInt(5),
				}),
			},
		},
		{
			name: "yo dawg I heard you like tuples inside your tuples, so I put a tuple inside your tuple inside your tuple...",
			args: args{
				v: MakeTuple([]Value{
					MakeTuple([]Value{
						MakeNull(),
						MakePhantom(),
						MakeTuple([]Value{
							MakeInt(TupleDelimiter),
							MakeInt(TupleIdentifier),
							MakeInt(-19252),
							MakeTuple([]Value{
								MakeBool(false),
								MakeFloat(284.12828),
								MakeBool(true),
								MakeString(string([]byte{byte(TupleIdentifier), byte(TupleDelimiter)})),
								MakeString("ale tupelki"),
								MakeTuple([]Value{
									MakeTime(time.Now()),
									MakeDuration(TupleDelimiter),
									MakeDuration(TupleIdentifier),
									MakeDuration(StringIdentifier),
									MakeDuration(StringDelimiter),
									MakeFloat(-1.284),
									MakeTuple(nil),
								}),
							}),
							MakeBool(false),
							MakePhantom(),
							MakeObject(map[string]Value{
								"a": MakeInt(17),
								"b": MakeString("ala ma kota"),
								"c": MakePhantom(),
							}),
							MakeString("tutaj też coś"),
						}),
						MakeInt(TupleDelimiter),
						MakeString("pa tera"),
					}),
					MakeInt(TupleDelimiter),
					MakeString("XD"),
				}),
			},
		},
		/* objects test */
		{
			name: "simple object",
			args: args{
				v: MakeObject(map[string]Value{
					"ala":     MakeInt(17),
					"ma":      MakePhantom(),
					"kota":    MakeBool(false),
					"i":       MakeString("psa"),
					"ale":     MakeFloat(17.17283),
					"nie":     MakeTime(time.Now()),
					"posiada": MakeNull(),
					"papugi": MakeTuple([]Value{
						MakeInt(11),
						MakeString("no i cześć"),
					}),
				}),
			},
		},
		{
			name: "complex object",
			args: args{
				v: MakeObject(map[string]Value{
					"object": MakeObject(map[string]Value{
						"a": MakePhantom(),
						"b": MakeString("ma kota"),
						"c": MakeInt(1283),
					}),
					"other": MakeTime(time.Now()),
					"another": MakeTuple([]Value{
						MakeString("xD"),
						MakeInt(18),
						MakeFloat(19.19238),
					}),
				}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b Value

			bytes := (&tt.args.v).MonotonicMarshal()

			err := b.MonotonicUnmarshal(bytes)
			if err != nil {
				t.Errorf("MonotonicMarshal() error = %v, wantErr false", err)
				return
			}

			if !reflect.DeepEqual(tt.args.v, b) {
				t.Errorf("Values are not equal!")
			}
		})
	}
}

func TestIfMarshalMonotonic(t *testing.T) {
	type args struct {
		values []Value
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "ints test",
			args: args{
				values: []Value{
					MakeInt(math.MinInt64),
					MakeInt(-456),
					MakeInt(-256),
					MakeInt(-255),
					MakeInt(-254),
					MakeInt(-253),
					MakeInt(-189),
					MakeInt(-10),
					MakeInt(0),
					MakeInt(1),
					MakeInt(3),
					MakeInt(17),
					MakeInt(254),
					MakeInt(255),
					MakeInt(256),
					MakeInt(257),
					MakeInt(24287),
					MakeInt(math.MaxInt64),
				},
			},
		},
		{
			name: "bool test",
			args: args{
				values: []Value{
					MakeBool(false),
					MakeBool(true),
				},
			},
		},
		{
			name: "strings test",
			args: args{
				values: []Value{
					MakeString("ala"),
					MakeString("ala ma kota"),
					MakeString("ala ma psa"),
					MakeString("bartek ma papugi"),
					MakeString("zebra"),
				},
			},
		},
		{
			name: "timestamp test",
			args: args{
				values: []Value{
					MakeTime(time.Date(2017, 11, 19, 22, 17, 38, 0, time.UTC)),
					MakeTime(time.Date(2017, 11, 19, 22, 17, 39, 0, time.UTC)),
					MakeTime(time.Date(2017, 11, 19, 22, 18, 0, 0, time.UTC)),
					MakeTime(time.Now()),
				},
			},
		},
		{
			name: "duration test",
			args: args{
				values: []Value{
					MakeDuration(0),
					MakeDuration(17),
					MakeDuration(254),
					MakeDuration(255),
					MakeDuration(256),
					MakeDuration(11923),
					MakeDuration(192308),
					MakeDuration(192309),
					MakeDuration(20000000),
				},
			},
		},
		{
			name: "tuple test",
			args: args{
				values: []Value{
					MakeTuple([]Value{
						MakeInt(17),
						MakeString("ala ma kota"),
						MakeBool(false),
					}),
					MakeTuple([]Value{
						MakeInt(18),
						MakeString("abcdef"),
						MakeBool(true),
					}),
					MakeTuple([]Value{
						MakeInt(18),
						MakeString("abcdefghij"),
						MakeBool(false),
					}),
					MakeTuple([]Value{
						MakeInt(18),
						MakeString("b"),
						MakeBool(false),
					}),
					MakeTuple([]Value{
						MakeInt(18),
						MakeString("b"),
						MakeBool(true),
					}),
				},
			},
		},
		{
			name: "float test",
			args: args{
				values: []Value{
					MakeFloat(math.Inf(-1)),
					MakeFloat(-20000000),
					MakeFloat(-192309),
					MakeFloat(-192308),
					MakeFloat(-11923),
					MakeFloat(-256),
					MakeFloat(-255),
					MakeFloat(-254.8),
					MakeFloat(-254.7),
					MakeFloat(-254.5),
					MakeFloat(-254.3),
					MakeFloat(-254.2),
					MakeFloat(-254.1),
					MakeFloat(-254),
					MakeFloat(-17),
					MakeFloat(-1.0 / 3.0),
					MakeFloat(-math.SmallestNonzeroFloat64),
					MakeFloat(0),
					MakeFloat(math.SmallestNonzeroFloat64),
					MakeFloat(1.0 / 3.0),
					MakeFloat(17),
					MakeFloat(254),
					MakeFloat(254.1),
					MakeFloat(254.2),
					MakeFloat(254.3),
					MakeFloat(254.5),
					MakeFloat(254.7),
					MakeFloat(254.8),
					MakeFloat(255),
					MakeFloat(256),
					MakeFloat(11923),
					MakeFloat(192308),
					MakeFloat(192309),
					MakeFloat(20000000),
					MakeFloat(math.MaxFloat64),
					MakeFloat(math.Inf(1)),
					MakeFloat(math.NaN()),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			argCount := len(tt.args.values)
			marshaled := make([][]byte, argCount)

			for i := 0; i < argCount; i++ {
				bytes := monotonicMarshal(&tt.args.values[i])
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

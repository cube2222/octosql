package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestDistinct_Get(t *testing.T) {
	type args struct {
		stream RecordStream
	}
	tests := []struct {
		name string
		args args
		want RecordStream
	}{
		{
			name: "simple test - all distinct",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{2, 10}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{3, 2}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7}),
				UtilNewRecord(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{3, 2}),
				UtilNewRecord(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{2, 10}),
			}),
		},

		{
			name: "same record every time",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7}),
			}),
		},

		{
			name: "similar records - int",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"id"},
						[]interface{}{1}),
					UtilNewRecord(
						[]octosql.VariableName{"id"},
						[]interface{}{2}),
					UtilNewRecord(
						[]octosql.VariableName{"id"},
						[]interface{}{3}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"id"},
					[]interface{}{1}),
				UtilNewRecord(
					[]octosql.VariableName{"id"},
					[]interface{}{2}),
				UtilNewRecord(
					[]octosql.VariableName{"id"},
					[]interface{}{3}),
			}),
		},

		{
			name: "reduction - multiple records",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
					UtilNewRecord(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"}),
					UtilNewRecord(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"}),
					UtilNewRecord(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
					UtilNewRecord(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Africa", "lion"}),
				UtilNewRecord(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Europe", "pigeon"}),
			}),
		},

		{
			name: "a bit of reduction - one record",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{2, "Kuba"}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{3, "Wojtek"}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"}),
					UtilNewRecord(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{4, "Wojtek"}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{1, "Janek"}),
				UtilNewRecord(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{2, "Kuba"}),
				UtilNewRecord(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{3, "Wojtek"}),
				UtilNewRecord(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{4, "Wojtek"}),
			}),
		},

		{
			name: "advanced data (slice) - no repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{1, 2, 3}}),
					UtilNewRecord(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{4, 5, 6, 7}}),
					UtilNewRecord(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{7, 8}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{4, 5, 6, 7}}),
				UtilNewRecord(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{1, 2, 3}}),
				UtilNewRecord(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{7, 8}}),
			}),
		},

		{
			name: "advanced data (slice) - repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{1, 2, 3}}),
					UtilNewRecord(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{1, 2, 3}}),
					UtilNewRecord(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{7, 8}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{1, 2, 3}}),
				UtilNewRecord(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{7, 8}}),
			}),
		},

		{
			name: "advanced (map) - repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}}),
					UtilNewRecord(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							1: "bbb",
							0: "aaa",
							2: "ccc",
						}}),
					UtilNewRecord(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccd",
						}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"map"},
					[]interface{}{map[int]string{
						1: "bbb",
						0: "aaa",
						2: "ccc",
					}}),
				UtilNewRecord(
					[]octosql.VariableName{"map"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccd",
					}}),
			}),
		},

		{
			name: "advanced (map + slice) - no repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					UtilNewRecord(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 2}}),
					UtilNewRecord(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							1: "bbb",
							0: "aaa",
							2: "ccc",
						}, []int{1, 3}}),
					UtilNewRecord(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccd",
						}, []int{1, 2}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				UtilNewRecord(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, []int{1, 2}}),
				UtilNewRecord(
					[]octosql.VariableName{"map"},
					[]interface{}{map[int]string{
						1: "bbb",
						0: "aaa",
						2: "ccc",
					}, []int{1, 3}}),
				UtilNewRecord(
					[]octosql.VariableName{"map"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccd",
					}, []int{1, 2}}),
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			distinct := DistinctStream{
				stream:    tt.args.stream,
				variables: nil,
				records:   newRecordSet(),
			}

			equal, err := AreStreamsEqual(tt.want, distinct)
			if err != nil {
				t.Errorf("Error in AreStreamsEqual()")
				return
			}

			if !equal {
				t.Errorf("Streams don't match")
				return
			}
		})
	}
}

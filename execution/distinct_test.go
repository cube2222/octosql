package execution

import (
	"testing"
	"time"

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
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{2, 10}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{3, 2}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{3, 2}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{2, 10}),
			}),
		},

		{
			name: "same record every time",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7}),
			}),
		},

		{
			name: "similar records - int",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"id"},
						[]interface{}{1}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id"},
						[]interface{}{2}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id"},
						[]interface{}{3}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"id"},
					[]interface{}{1}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id"},
					[]interface{}{2}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id"},
					[]interface{}{3}),
			}),
		},

		{
			name: "reduction - multiple records",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Africa", "lion"}),
				NewRecordFromSlice(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Europe", "pigeon"}),
			}),
		},

		{
			name: "a bit of reduction - one record",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{2, "Kuba"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{3, "Wojtek"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{4, "Wojtek"}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{1, "Janek"}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{2, "Kuba"}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{3, "Wojtek"}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{4, "Wojtek"}),
			}),
		},

		{
			name: "advanced data (slice) - no repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{1, 2, 3}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{4, 5, 6, 7}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{7, 8}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{4, 5, 6, 7}}),
				NewRecordFromSlice(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{1, 2, 3}}),
				NewRecordFromSlice(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{7, 8}}),
			}),
		},

		{
			name: "advanced data (slice) - repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{1, 2, 3}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{1, 2, 3}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]int{7, 8}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{1, 2, 3}}),
				NewRecordFromSlice(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]int{7, 8}}),
			}),
		},

		{
			name: "advanced (map) - repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							1: "bbb",
							0: "aaa",
							2: "ccc",
						}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"map"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccd",
						}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"map"},
					[]interface{}{map[int]string{
						1: "bbb",
						0: "aaa",
						2: "ccc",
					}}),
				NewRecordFromSlice(
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
					NewRecordFromSlice(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 2}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[int]string{
							1: "bbb",
							0: "aaa",
							2: "ccc",
						}, []int{1, 3}}),
					NewRecordFromSlice(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccd",
						}, []int{1, 2}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, []int{1, 2}}),
				NewRecordFromSlice(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[int]string{
						1: "bbb",
						0: "aaa",
						2: "ccc",
					}, []int{1, 3}}),
				NewRecordFromSlice(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccd",
					}, []int{1, 2}}),
			}),
		},

		{
			name: "the ultimate test",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice( //unique
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 2}, 7, "nazwa"}),
					NewRecordFromSlice( //unique - different column order
						[]octosql.VariableName{"map", "name", "number", "numbers"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, "nazwa", 7, []int{1, 2}}),
					NewRecordFromSlice( //unique - diff in numbers
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 3}, 7, "nazwa"}),
					NewRecordFromSlice( //unique diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 3}, 8, "nazwa"}),
					NewRecordFromSlice( //unique - diff in name
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 3}, 7, "nazwa0"}),
					NewRecordFromSlice( //unique - diff in map
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "cccc",
						}, []int{1, 3}, 7, "nazwa"}),
					NewRecordFromSlice( //repetition of diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[int]string{
							0: "aaa",
							1: "bbb",
							2: "ccc",
						}, []int{1, 3}, 8, "nazwa"}),
					NewRecordFromSlice( //unique - second row template
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}}),
					NewRecordFromSlice( //unique - second row template; diff in scores
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{9, 1, 11}}),
					NewRecordFromSlice( //repetition - second row template;
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}}),
					NewRecordFromSlice( //unique - second row template; mixed columns same values
						[]octosql.VariableName{"id", "scores", "age"},
						[]interface{}{[]interface{}{9, 1, 11}, 1, 17}),
				}),
			},

			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice( //unique
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, []int{1, 2}, 7, "nazwa"}),
				NewRecordFromSlice( //unique - different column order
					[]octosql.VariableName{"map", "name", "number", "numbers"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, "nazwa", 7, []int{1, 2}}),
				NewRecordFromSlice( //unique - diff in numbers
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, []int{1, 3}, 7, "nazwa"}),
				NewRecordFromSlice( //unique diff in number
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, []int{1, 3}, 8, "nazwa"}),
				NewRecordFromSlice( //unique - diff in name
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "ccc",
					}, []int{1, 3}, 7, "nazwa0"}),
				NewRecordFromSlice( //unique - diff in map
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[int]string{
						0: "aaa",
						1: "bbb",
						2: "cccc",
					}, []int{1, 3}, 7, "nazwa"}),
				NewRecordFromSlice( //unique - second row template
					[]octosql.VariableName{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{1, 9, 11}}),
				NewRecordFromSlice( //unique - second row template; diff in scores
					[]octosql.VariableName{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{9, 1, 11}}),
				NewRecordFromSlice( //unique - second row template; mixed columns same values
					[]octosql.VariableName{"id", "scores", "age"},
					[]interface{}{[]interface{}{9, 1, 11}, 1, 17}),
			}),
		},

		{
			name: "time.Time",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSlice(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSlice(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSlice(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2018, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSlice(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 12, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSlice(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 27, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"date"},
					[]interface{}{time.Date(2018, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSlice(
					[]octosql.VariableName{"date"},
					[]interface{}{time.Date(2019, 12, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSlice(
					[]octosql.VariableName{"date"},
					[]interface{}{time.Date(2019, 11, 27, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSlice(
					[]octosql.VariableName{"date"},
					[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSlice(
					[]octosql.VariableName{"date"},
					[]interface{}{time.Date(2019, 11, 28, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
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

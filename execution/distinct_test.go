package execution

import (
	"context"
	"testing"
	"time"
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
					NewRecordFromSliceWithNormalize(
						[]string{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "age"},
						[]interface{}{2, 10}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "age"},
						[]interface{}{3, 2}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"id", "age"},
					[]interface{}{1, 7}),
				NewRecordFromSliceWithNormalize(
					[]string{"id", "age"},
					[]interface{}{3, 2}),
				NewRecordFromSliceWithNormalize(
					[]string{"id", "age"},
					[]interface{}{2, 10}),
			}),
		},

		{
			name: "same record every time",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "age"},
						[]interface{}{1, 7}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"id", "age"},
					[]interface{}{1, 7}),
			}),
		},

		{
			name: "similar records - int",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"id"},
						[]interface{}{1}),
					NewRecordFromSliceWithNormalize(
						[]string{"id"},
						[]interface{}{2}),
					NewRecordFromSliceWithNormalize(
						[]string{"id"},
						[]interface{}{3}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"id"},
					[]interface{}{1}),
				NewRecordFromSliceWithNormalize(
					[]string{"id"},
					[]interface{}{2}),
				NewRecordFromSliceWithNormalize(
					[]string{"id"},
					[]interface{}{3}),
			}),
		},

		{
			name: "reduction - multiple records",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
					NewRecordFromSliceWithNormalize(
						[]string{"continent", "name"},
						[]interface{}{"Europe", "pigeon"}),
					NewRecordFromSliceWithNormalize(
						[]string{"continent", "name"},
						[]interface{}{"Europe", "pigeon"}),
					NewRecordFromSliceWithNormalize(
						[]string{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
					NewRecordFromSliceWithNormalize(
						[]string{"continent", "name"},
						[]interface{}{"Africa", "lion"}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"continent", "name"},
					[]interface{}{"Africa", "lion"}),
				NewRecordFromSliceWithNormalize(
					[]string{"continent", "name"},
					[]interface{}{"Europe", "pigeon"}),
			}),
		},

		{
			name: "a bit of reduction - one record",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"id", "name"},
						[]interface{}{1, "Janek"}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "name"},
						[]interface{}{2, "Kuba"}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "name"},
						[]interface{}{3, "Wojtek"}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "name"},
						[]interface{}{1, "Janek"}),
					NewRecordFromSliceWithNormalize(
						[]string{"id", "name"},
						[]interface{}{4, "Wojtek"}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"id", "name"},
					[]interface{}{1, "Janek"}),
				NewRecordFromSliceWithNormalize(
					[]string{"id", "name"},
					[]interface{}{2, "Kuba"}),
				NewRecordFromSliceWithNormalize(
					[]string{"id", "name"},
					[]interface{}{3, "Wojtek"}),
				NewRecordFromSliceWithNormalize(
					[]string{"id", "name"},
					[]interface{}{4, "Wojtek"}),
			}),
		},

		{
			name: "advanced data (slice) - no repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}}),
					NewRecordFromSliceWithNormalize(
						[]string{"numbers"},
						[]interface{}{[]interface{}{4, 5, 6, 7}}),
					NewRecordFromSliceWithNormalize(
						[]string{"numbers"},
						[]interface{}{[]interface{}{7, 8}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"numbers"},
					[]interface{}{[]interface{}{4, 5, 6, 7}}),
				NewRecordFromSliceWithNormalize(
					[]string{"numbers"},
					[]interface{}{[]interface{}{1, 2, 3}}),
				NewRecordFromSliceWithNormalize(
					[]string{"numbers"},
					[]interface{}{[]interface{}{7, 8}}),
			}),
		},

		{
			name: "advanced data (slice) - repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}}),
					NewRecordFromSliceWithNormalize(
						[]string{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}}),
					NewRecordFromSliceWithNormalize(
						[]string{"numbers"},
						[]interface{}{[]interface{}{7, 8}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"numbers"},
					[]interface{}{[]interface{}{1, 2, 3}}),
				NewRecordFromSliceWithNormalize(
					[]string{"numbers"},
					[]interface{}{[]interface{}{7, 8}}),
			}),
		},

		{
			name: "advanced (map) - repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"map"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}}),
					NewRecordFromSliceWithNormalize(
						[]string{"map"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}}),
					NewRecordFromSliceWithNormalize(
						[]string{"map"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccd",
						}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"map"},
					[]interface{}{map[string]interface{}{
						"1": "bbb",
						"0": "aaa",
						"2": "ccc",
					}}),
				NewRecordFromSliceWithNormalize(
					[]string{"map"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccd",
					}}),
			}),
		},

		{
			name: "advanced (map + slice) - no repetitions",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 2}}),
					NewRecordFromSliceWithNormalize(
						[]string{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}, []interface{}{1, 3}}),
					NewRecordFromSliceWithNormalize(
						[]string{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccd",
						}, []interface{}{1, 2}}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 2}}),
				NewRecordFromSliceWithNormalize(
					[]string{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"1": "bbb",
						"0": "aaa",
						"2": "ccc",
					}, []interface{}{1, 3}}),
				NewRecordFromSliceWithNormalize(
					[]string{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccd",
					}, []interface{}{1, 2}}),
			}),
		},

		{
			name: "the ultimate test",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize( //unique
						[]string{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 2}, 7, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique - different column order
						[]string{"map", "name", "number", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, "nazwa", 7, []interface{}{1, 2}}),
					NewRecordFromSliceWithNormalize( //unique - diff in numbers
						[]string{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique diff in number
						[]string{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique - diff in name
						[]string{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa0"}),
					NewRecordFromSliceWithNormalize( //unique - diff in map
						[]string{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "cccc",
						}, []interface{}{1, 3}, 7, "nazwa"}),
					NewRecordFromSliceWithNormalize( //repetition of diff in number
						[]string{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique - second row template
						[]string{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}}),
					NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
						[]string{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{9, 1, 11}}),
					NewRecordFromSliceWithNormalize( //repetition - second row template;
						[]string{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}}),
					NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
						[]string{"id", "scores", "age"},
						[]interface{}{[]interface{}{9, 1, 11}, 1, 17}),
				}),
			},

			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize( //unique
					[]string{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 2}, 7, "nazwa"}),
				NewRecordFromSliceWithNormalize( //unique - different column order
					[]string{"map", "name", "number", "numbers"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, "nazwa", 7, []interface{}{1, 2}}),
				NewRecordFromSliceWithNormalize( //unique - diff in numbers
					[]string{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 7, "nazwa"}),
				NewRecordFromSliceWithNormalize( //unique diff in number
					[]string{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 8, "nazwa"}),
				NewRecordFromSliceWithNormalize( //unique - diff in name
					[]string{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 7, "nazwa0"}),
				NewRecordFromSliceWithNormalize( //unique - diff in map
					[]string{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "cccc",
					}, []interface{}{1, 3}, 7, "nazwa"}),
				NewRecordFromSliceWithNormalize( //unique - second row template
					[]string{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{1, 9, 11}}),
				NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
					[]string{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{9, 1, 11}}),
				NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
					[]string{"id", "scores", "age"},
					[]interface{}{[]interface{}{9, 1, 11}, 1, 17}),
			}),
		},

		{
			name: "time.Time",
			args: args{
				NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]string{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]string{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]string{"date"},
						[]interface{}{time.Date(2018, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]string{"date"},
						[]interface{}{time.Date(2019, 12, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]string{"date"},
						[]interface{}{time.Date(2019, 11, 27, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]string{"date"},
					[]interface{}{time.Date(2018, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSliceWithNormalize(
					[]string{"date"},
					[]interface{}{time.Date(2019, 12, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSliceWithNormalize(
					[]string{"date"},
					[]interface{}{time.Date(2019, 11, 27, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSliceWithNormalize(
					[]string{"date"},
					[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
				NewRecordFromSliceWithNormalize(
					[]string{"date"},
					[]interface{}{time.Date(2019, 11, 28, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			distinct := NewDistinctStream(tt.args.stream, nil, newRecordSet())

			_ = NewRecordFromSliceWithNormalize(
				[]string{"id", "name"},
				[]interface{}{1, "Janek"})

			stateStorage := GetTestStorage(t)

			equal, err := AreStreamsEqualNoOrdering(context.Background(), stateStorage, tt.want, distinct)
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

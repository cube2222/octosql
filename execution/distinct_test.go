package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestDistinct_Get(t *testing.T) {
	type args struct {
		source Node
	}
	tests := []struct {
		name string
		args args
		want RecordStream
	}{
		{
			name: "simple test - all distinct",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{2, 10},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{3, 2},
						WithID(NewID("id3"))),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{2, 10},
					WithID(NewID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{3, 2},
					WithID(NewID("id3"))),
			}),
		},

		{
			name: "same record every time",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewID("id3"))),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7},
					WithID(NewID("id1"))),
			}),
		},

		{
			name: "similar records - int",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id"},
						[]interface{}{1},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id"},
						[]interface{}{2},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id"},
						[]interface{}{3},
						WithID(NewID("id3"))),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id"},
					[]interface{}{1},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id"},
					[]interface{}{2},
					WithID(NewID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id"},
					[]interface{}{3},
					WithID(NewID("id3"))),
			}),
		},

		{
			name: "reduction - multiple records",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"},
						WithID(NewID("id3"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"},
						WithID(NewID("id4"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"},
						WithID(NewID("id5"))),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Africa", "lion"},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Europe", "pigeon"},
					WithID(NewID("id3"))),
			}),
		},

		{
			name: "a bit of reduction - one record",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{2, "Kuba"},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{3, "Wojtek"},
						WithID(NewID("id3"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"},
						WithID(NewID("id4"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{4, "Wojtek"},
						WithID(NewID("id5"))),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{1, "Janek"},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{2, "Kuba"},
					WithID(NewID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{3, "Wojtek"},
					WithID(NewID("id3"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{4, "Wojtek"},
					WithID(NewID("id5"))),
			}),
		},

		{
			name: "advanced data (slice) - no repetitions",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2}},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1}},
						WithID(NewID("id3"))),
				}),
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1, 2, 3}},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1, 2}},
					WithID(NewID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1}},
					WithID(NewID("id3"))),
			}),
		},
		/*
			{
				name: "advanced data (slice) - repetitions",
				args: args{
					source: NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"numbers"},
							[]interface{}{[]interface{}{1, 2, 3}}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"numbers"},
							[]interface{}{[]interface{}{1, 2, 3}}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"numbers"},
							[]interface{}{[]interface{}{7, 8}}),
					}),
				},
				want: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{7, 8}}),
				}),
			},

			{
				name: "advanced (map) - repetitions",
				args: args{
					source: NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"map"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"map"},
							[]interface{}{map[string]interface{}{
								"1": "bbb",
								"0": "aaa",
								"2": "ccc",
							}}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"map"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccd",
							}}),
					}),
				},
				want: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map"},
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
					source: NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"map", "numbers"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, []interface{}{1, 2}}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"map", "numbers"},
							[]interface{}{map[string]interface{}{
								"1": "bbb",
								"0": "aaa",
								"2": "ccc",
							}, []interface{}{1, 3}}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"map", "numbers"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccd",
							}, []interface{}{1, 2}}),
					}),
				},
				want: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 2}}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}, []interface{}{1, 3}}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
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
					source: NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize( //unique
							[]octosql.VariableName{"map", "numbers", "number", "name"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, []interface{}{1, 2}, 7, "nazwa"}),
						NewRecordFromSliceWithNormalize( //unique - different column order
							[]octosql.VariableName{"map", "name", "number", "numbers"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, "nazwa", 7, []interface{}{1, 2}}),
						NewRecordFromSliceWithNormalize( //unique - diff in numbers
							[]octosql.VariableName{"map", "numbers", "number", "name"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, []interface{}{1, 3}, 7, "nazwa"}),
						NewRecordFromSliceWithNormalize( //unique diff in number
							[]octosql.VariableName{"map", "numbers", "number", "name"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, []interface{}{1, 3}, 8, "nazwa"}),
						NewRecordFromSliceWithNormalize( //unique - diff in name
							[]octosql.VariableName{"map", "numbers", "number", "name"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, []interface{}{1, 3}, 7, "nazwa0"}),
						NewRecordFromSliceWithNormalize( //unique - diff in map
							[]octosql.VariableName{"map", "numbers", "number", "name"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "cccc",
							}, []interface{}{1, 3}, 7, "nazwa"}),
						NewRecordFromSliceWithNormalize( //repetition of diff in number
							[]octosql.VariableName{"map", "numbers", "number", "name"},
							[]interface{}{map[string]interface{}{
								"0": "aaa",
								"1": "bbb",
								"2": "ccc",
							}, []interface{}{1, 3}, 8, "nazwa"}),
						NewRecordFromSliceWithNormalize( //unique - second row template
							[]octosql.VariableName{"id", "age", "scores"},
							[]interface{}{1, 17, []interface{}{1, 9, 11}}),
						NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
							[]octosql.VariableName{"id", "age", "scores"},
							[]interface{}{1, 17, []interface{}{9, 1, 11}}),
						NewRecordFromSliceWithNormalize( //repetition - second row template;
							[]octosql.VariableName{"id", "age", "scores"},
							[]interface{}{1, 17, []interface{}{1, 9, 11}}),
						NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
							[]octosql.VariableName{"id", "scores", "age"},
							[]interface{}{[]interface{}{9, 1, 11}, 1, 17}),
					}),
				},

				want: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize( //unique
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 2}, 7, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique - different column order
						[]octosql.VariableName{"map", "name", "number", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, "nazwa", 7, []interface{}{1, 2}}),
					NewRecordFromSliceWithNormalize( //unique - diff in numbers
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique - diff in name
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa0"}),
					NewRecordFromSliceWithNormalize( //unique - diff in map
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "cccc",
						}, []interface{}{1, 3}, 7, "nazwa"}),
					NewRecordFromSliceWithNormalize( //unique - second row template
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}}),
					NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{9, 1, 11}}),
					NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
						[]octosql.VariableName{"id", "scores", "age"},
						[]interface{}{[]interface{}{9, 1, 11}, 1, 17}),
				}),
			},

			{
				name: "time.Time",
				args: args{
					source: NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"date"},
							[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"date"},
							[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"date"},
							[]interface{}{time.Date(2019, 11, 28, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"date"},
							[]interface{}{time.Date(2018, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"date"},
							[]interface{}{time.Date(2019, 12, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{"date"},
							[]interface{}{time.Date(2019, 11, 27, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
					}),
				},
				want: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2018, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 12, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 27, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 0, 0, time.FixedZone("Poland", 0))}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"},
						[]interface{}{time.Date(2019, 11, 28, 0, 0, 2, 0, time.FixedZone("Poland", 0))}),
				}),
			}, */
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := GetTestStorage(t)
			ctx := context.Background()

			distinct := NewDistinct(stateStorage, tt.args.source, "")

			tx := stateStorage.BeginTransaction()
			stream, _, err := distinct.Get(storage.InjectStateTransaction(context.Background(), tx), octosql.NoVariables(), GetRawStreamID())
			if err != nil {
				t.Fatal(err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			ok, err := AreStreamsEqualNoOrdering(ctx, stateStorage, stream, tt.want)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("streams not equal")
			}
		})
	}
}

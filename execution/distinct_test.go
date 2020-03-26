package execution

import (
	"context"
	"testing"
	"time"

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
		want []*Record
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
			want: []*Record{
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
			},
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
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7},
					WithID(NewID("id1"))),
			},
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
			want: []*Record{
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
			},
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
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Africa", "lion"},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Europe", "pigeon"},
					WithID(NewID("id3"))),
			},
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
			want: []*Record{
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
			},
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
			want: []*Record{
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
			},
		},

		{
			name: "advanced data (slice) - repetitions",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{7, 8}},
						WithID(NewID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1, 2, 3}},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{7, 8}},
					WithID(NewID("id3"))),
			},
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
						}},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccd",
						}},
						WithID(NewID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map"},
					[]interface{}{map[string]interface{}{
						"1": "bbb",
						"0": "aaa",
						"2": "ccc",
					}},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccd",
					}},
					WithID(NewID("id3"))),
			},
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
						}, []interface{}{1, 2}},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}, []interface{}{1, 3}},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccd",
						}, []interface{}{1, 2}},
						WithID(NewID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 2}},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"1": "bbb",
						"0": "aaa",
						"2": "ccc",
					}, []interface{}{1, 3}},
					WithID(NewID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccd",
					}, []interface{}{1, 2}},
					WithID(NewID("id3"))),
			},
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
						}, []interface{}{1, 2}, 7, "nazwa"},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize( //repetition - different column order
						[]octosql.VariableName{"map", "numbers", "name", "number"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 2}, "nazwa", 7},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize( //unique - diff in numbers
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa"},
						WithID(NewID("id3"))),
					NewRecordFromSliceWithNormalize( //unique diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"},
						WithID(NewID("id4"))),
					NewRecordFromSliceWithNormalize( //unique - diff in name
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa0"},
						WithID(NewID("id5"))),
					NewRecordFromSliceWithNormalize( //unique - diff in map
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "cccc",
						}, []interface{}{1, 3}, 7, "nazwa"},
						WithID(NewID("id6"))),
					NewRecordFromSliceWithNormalize( //repetition of diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"},
						WithID(NewID("id7"))),
					NewRecordFromSliceWithNormalize( //unique - second row template
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}},
						WithID(NewID("id8"))),
					NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{9, 1, 11}},
						WithID(NewID("id9"))),
					NewRecordFromSliceWithNormalize( //repetition - second row template;
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}},
						WithID(NewID("id10"))),
					NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
						[]octosql.VariableName{"id", "scores", "age"},
						[]interface{}{[]interface{}{9, 1, 11}, 1, 17},
						WithID(NewID("id11"))),
				}),
			},

			want: []*Record{
				NewRecordFromSliceWithNormalize( //unique
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 2}, 7, "nazwa"},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize( //unique - diff in numbers
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 7, "nazwa"},
					WithID(NewID("id3"))),
				NewRecordFromSliceWithNormalize( //unique diff in number
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 8, "nazwa"},
					WithID(NewID("id4"))),
				NewRecordFromSliceWithNormalize( //unique - diff in name
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 7, "nazwa0"},
					WithID(NewID("id5"))),
				NewRecordFromSliceWithNormalize( //unique - diff in map
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "cccc",
					}, []interface{}{1, 3}, 7, "nazwa"},
					WithID(NewID("id6"))),
				NewRecordFromSliceWithNormalize( //unique - second row template
					[]octosql.VariableName{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{1, 9, 11}},
					WithID(NewID("id8"))),
				NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
					[]octosql.VariableName{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{9, 1, 11}},
					WithID(NewID("id9"))),
				NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
					[]octosql.VariableName{"id", "scores", "age"},
					[]interface{}{[]interface{}{9, 1, 11}, 1, 17},
					WithID(NewID("id11"))),
			},
		},

		{
			name: "time.Time",
			args: args{
				source: NewDummyNode([]*Record{ // base is 2018/4/20/1/1/1/1
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique year
						[]interface{}{time.Date(2019, 4, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique month
						[]interface{}{time.Date(2018, 5, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique day
						[]interface{}{time.Date(2018, 4, 21, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id3"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique hour
						[]interface{}{time.Date(2018, 4, 20, 2, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id4"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique minute
						[]interface{}{time.Date(2018, 4, 20, 1, 2, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id5"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique second
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 2, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id6"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique nanosecond
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 1, 2, time.FixedZone("Poland", 0))},
						WithID(NewID("id7"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // year repetition
						[]interface{}{time.Date(2019, 4, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id8"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // month repetition
						[]interface{}{time.Date(2018, 5, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id9"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // day repetition
						[]interface{}{time.Date(2018, 4, 21, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id10"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // hour repetition
						[]interface{}{time.Date(2018, 4, 20, 2, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id11"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // minute repetition
						[]interface{}{time.Date(2018, 4, 20, 1, 2, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id12"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // second repetition
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 2, 1, time.FixedZone("Poland", 0))},
						WithID(NewID("id13"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // nanosecond repetition
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 1, 2, time.FixedZone("Poland", 0))},
						WithID(NewID("id14"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique year
					[]interface{}{time.Date(2019, 4, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique month
					[]interface{}{time.Date(2018, 5, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique day
					[]interface{}{time.Date(2018, 4, 21, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewID("id3"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique hour
					[]interface{}{time.Date(2018, 4, 20, 2, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewID("id4"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique minute
					[]interface{}{time.Date(2018, 4, 20, 1, 2, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewID("id5"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique second
					[]interface{}{time.Date(2018, 4, 20, 1, 1, 2, 1, time.FixedZone("Poland", 0))},
					WithID(NewID("id6"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique nanosecond
					[]interface{}{time.Date(2018, 4, 20, 1, 1, 1, 2, time.FixedZone("Poland", 0))},
					WithID(NewID("id7"))),
			},
		},
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

			wantStream := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), tt.want)

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			err = AreStreamsEqualNoOrdering(ctx, stateStorage, stream, wantStream)
			if err != nil {
				t.Fatal(err)
			}

		})
	}
}

package execution

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
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
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{2, 10},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{3, 2},
						WithID(NewRecordID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{2, 10},
					WithID(NewRecordID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{3, 2},
					WithID(NewRecordID("id3"))),
			},
		},

		{
			name: "same record every time",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7},
						WithID(NewRecordID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7},
					WithID(NewRecordID("id1"))),
			},
		},

		{
			name: "similar records - int",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id"},
						[]interface{}{1},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id"},
						[]interface{}{2},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id"},
						[]interface{}{3},
						WithID(NewRecordID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id"},
					[]interface{}{1},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id"},
					[]interface{}{2},
					WithID(NewRecordID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id"},
					[]interface{}{3},
					WithID(NewRecordID("id3"))),
			},
		},

		{
			name: "reduction - multiple records",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"},
						WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Europe", "pigeon"},
						WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"continent", "name"},
						[]interface{}{"Africa", "lion"},
						WithID(NewRecordID("id5"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Africa", "lion"},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"continent", "name"},
					[]interface{}{"Europe", "pigeon"},
					WithID(NewRecordID("id3"))),
			},
		},

		{
			name: "a bit of reduction - one record",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{2, "Kuba"},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{3, "Wojtek"},
						WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{1, "Janek"},
						WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "name"},
						[]interface{}{4, "Wojtek"},
						WithID(NewRecordID("id5"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{1, "Janek"},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{2, "Kuba"},
					WithID(NewRecordID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{3, "Wojtek"},
					WithID(NewRecordID("id3"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "name"},
					[]interface{}{4, "Wojtek"},
					WithID(NewRecordID("id5"))),
			},
		},

		{
			name: "advanced data (slice) - no repetitions",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2}},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1}},
						WithID(NewRecordID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1, 2, 3}},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1, 2}},
					WithID(NewRecordID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1}},
					WithID(NewRecordID("id3"))),
			},
		},

		{
			name: "advanced data (slice) - repetitions",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{1, 2, 3}},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"numbers"},
						[]interface{}{[]interface{}{7, 8}},
						WithID(NewRecordID("id3"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{1, 2, 3}},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"numbers"},
					[]interface{}{[]interface{}{7, 8}},
					WithID(NewRecordID("id3"))),
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
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccd",
						}},
						WithID(NewRecordID("id3"))),
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
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccd",
					}},
					WithID(NewRecordID("id3"))),
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
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"1": "bbb",
							"0": "aaa",
							"2": "ccc",
						}, []interface{}{1, 3}},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"map", "numbers"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccd",
						}, []interface{}{1, 2}},
						WithID(NewRecordID("id3"))),
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
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"1": "bbb",
						"0": "aaa",
						"2": "ccc",
					}, []interface{}{1, 3}},
					WithID(NewRecordID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"map", "numbers"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccd",
					}, []interface{}{1, 2}},
					WithID(NewRecordID("id3"))),
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
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize( //repetition - different column order
						[]octosql.VariableName{"map", "numbers", "name", "number"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 2}, "nazwa", 7},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize( //unique - diff in numbers
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa"},
						WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize( //unique diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"},
						WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize( //unique - diff in name
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 7, "nazwa0"},
						WithID(NewRecordID("id5"))),
					NewRecordFromSliceWithNormalize( //unique - diff in map
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "cccc",
						}, []interface{}{1, 3}, 7, "nazwa"},
						WithID(NewRecordID("id6"))),
					NewRecordFromSliceWithNormalize( //repetition of diff in number
						[]octosql.VariableName{"map", "numbers", "number", "name"},
						[]interface{}{map[string]interface{}{
							"0": "aaa",
							"1": "bbb",
							"2": "ccc",
						}, []interface{}{1, 3}, 8, "nazwa"},
						WithID(NewRecordID("id7"))),
					NewRecordFromSliceWithNormalize( //unique - second row template
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}},
						WithID(NewRecordID("id8"))),
					NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{9, 1, 11}},
						WithID(NewRecordID("id9"))),
					NewRecordFromSliceWithNormalize( //repetition - second row template;
						[]octosql.VariableName{"id", "age", "scores"},
						[]interface{}{1, 17, []interface{}{1, 9, 11}},
						WithID(NewRecordID("id10"))),
					NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
						[]octosql.VariableName{"id", "scores", "age"},
						[]interface{}{[]interface{}{9, 1, 11}, 1, 17},
						WithID(NewRecordID("id11"))),
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
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize( //unique - diff in numbers
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 7, "nazwa"},
					WithID(NewRecordID("id3"))),
				NewRecordFromSliceWithNormalize( //unique diff in number
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 8, "nazwa"},
					WithID(NewRecordID("id4"))),
				NewRecordFromSliceWithNormalize( //unique - diff in name
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "ccc",
					}, []interface{}{1, 3}, 7, "nazwa0"},
					WithID(NewRecordID("id5"))),
				NewRecordFromSliceWithNormalize( //unique - diff in map
					[]octosql.VariableName{"map", "numbers", "number", "name"},
					[]interface{}{map[string]interface{}{
						"0": "aaa",
						"1": "bbb",
						"2": "cccc",
					}, []interface{}{1, 3}, 7, "nazwa"},
					WithID(NewRecordID("id6"))),
				NewRecordFromSliceWithNormalize( //unique - second row template
					[]octosql.VariableName{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{1, 9, 11}},
					WithID(NewRecordID("id8"))),
				NewRecordFromSliceWithNormalize( //unique - second row template; diff in scores
					[]octosql.VariableName{"id", "age", "scores"},
					[]interface{}{1, 17, []interface{}{9, 1, 11}},
					WithID(NewRecordID("id9"))),
				NewRecordFromSliceWithNormalize( //unique - second row template; mixed columns same values
					[]octosql.VariableName{"id", "scores", "age"},
					[]interface{}{[]interface{}{9, 1, 11}, 1, 17},
					WithID(NewRecordID("id11"))),
			},
		},

		{
			name: "time.Time",
			args: args{
				source: NewDummyNode([]*Record{ // base is 2018/4/20/1/1/1/1
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique year
						[]interface{}{time.Date(2019, 4, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique month
						[]interface{}{time.Date(2018, 5, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique day
						[]interface{}{time.Date(2018, 4, 21, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique hour
						[]interface{}{time.Date(2018, 4, 20, 2, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique minute
						[]interface{}{time.Date(2018, 4, 20, 1, 2, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id5"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique second
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 2, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id6"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // unique nanosecond
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 1, 2, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id7"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // year repetition
						[]interface{}{time.Date(2019, 4, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id8"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // month repetition
						[]interface{}{time.Date(2018, 5, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id9"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // day repetition
						[]interface{}{time.Date(2018, 4, 21, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id10"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // hour repetition
						[]interface{}{time.Date(2018, 4, 20, 2, 1, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id11"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // minute repetition
						[]interface{}{time.Date(2018, 4, 20, 1, 2, 1, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id12"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // second repetition
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 2, 1, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id13"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"date"}, // nanosecond repetition
						[]interface{}{time.Date(2018, 4, 20, 1, 1, 1, 2, time.FixedZone("Poland", 0))},
						WithID(NewRecordID("id14"))),
				}),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique year
					[]interface{}{time.Date(2019, 4, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id1"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique month
					[]interface{}{time.Date(2018, 5, 20, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id2"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique day
					[]interface{}{time.Date(2018, 4, 21, 1, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id3"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique hour
					[]interface{}{time.Date(2018, 4, 20, 2, 1, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id4"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique minute
					[]interface{}{time.Date(2018, 4, 20, 1, 2, 1, 1, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id5"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique second
					[]interface{}{time.Date(2018, 4, 20, 1, 1, 2, 1, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id6"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"date"}, // unique nanosecond
					[]interface{}{time.Date(2018, 4, 20, 1, 1, 1, 2, time.FixedZone("Poland", 0))},
					WithID(NewRecordID("id7"))),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)
			ctx := context.Background()

			distinct := NewDistinct(stateStorage, tt.args.source, "")

			stream := GetTestStream(t, stateStorage, octosql.NoVariables(), distinct)

			tx := stateStorage.BeginTransaction()
			wantStream := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), tt.want)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := AreStreamsEqualNoOrdering(ctx, stateStorage, stream, wantStream); err != nil {
				t.Fatal(err)
			}

			if err := stream.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close distinct stream: %v", err)
				return
			}
			if err := wantStream.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}

func TestDistinct_Retractions(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	ctx := context.Background()
	fields := []octosql.VariableName{"string", "number"}
	inputRecords := []*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 1}, WithID(NewRecordID("id1"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 2}, WithID(NewRecordID("id2"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 1}, WithID(NewRecordID("id3"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 1}, WithID(NewRecordID("id4")), WithUndo()), // retraction of a record that appears once
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 2}, WithID(NewRecordID("id5"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 2}, WithID(NewRecordID("id6")), WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 2}, WithID(NewRecordID("id7")), WithUndo()), // retraction of a record that appears twice
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 2}, WithID(NewRecordID("id8"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 2}, WithID(NewRecordID("id9")), WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 2}, WithID(NewRecordID("id10"))), // a record appears again after being retracted
		NewRecordFromSliceWithNormalize(fields, []interface{}{"c", 1}, WithID(NewRecordID("id11")), WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"c", 1}, WithID(NewRecordID("id12"))), // we don't want to send the (c,1) record at all
	}

	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 1}, WithID(NewRecordID("id1"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 2}, WithID(NewRecordID("id2"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 1}, WithID(NewRecordID("id3"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 1}, WithID(NewRecordID("id4")), WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"a", 2}, WithID(NewRecordID("id7")), WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 2}, WithID(NewRecordID("id8"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 2}, WithID(NewRecordID("id9")), WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"b", 2}, WithID(NewRecordID("id10"))),
	}

	source := NewDummyNode(inputRecords)

	distinct := NewDistinct(stateStorage, source, "")

	stream := GetTestStream(t, stateStorage, octosql.NoVariables(), distinct)

	tx := stateStorage.BeginTransaction()
	wantStream := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err := AreStreamsEqualNoOrdering(ctx, stateStorage, stream, wantStream)
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close distinct stream: %v", err)
		return
	}
	if err := wantStream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

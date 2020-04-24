package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestStreamJoin(t *testing.T) {
	leftFieldNames1 := []octosql.VariableName{"left.a", "left.b"}
	rightFieldNames1 := []octosql.VariableName{"right.a", "right.b"}
	concatFieldNames1 := append(leftFieldNames1, rightFieldNames1...)

	type fields struct {
		leftSource, rightSource Node
		leftKey, rightKey       []Expression
		joinType                JoinType
		eventTimeField          octosql.VariableName
		trigger                 TriggerPrototype
	}

	tests := []struct {
		name           string
		fields         fields
		executionCount int
		want           Node
	}{
		{
			name: "inner join 1 - no event time field, no retractions",
			fields: fields{
				leftSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 2}, WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"e", 2}, WithID(NewRecordID("id7"))),
				}),
				leftKey: []Expression{NewVariable("left.a")},

				rightSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 11}, WithID(NewRecordID("id5"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"b", 12}, WithID(NewRecordID("id6"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"f", 12}, WithID(NewRecordID("id8"))),
				}),
				rightKey: []Expression{NewVariable("right.a")},

				joinType:       INNER_JOIN,
				eventTimeField: "",
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"b", 2, "b", 12}),
			}),

			executionCount: 4, // the only non-determinism is the order
		},
		{
			name: "left join 1 - no event time field, no retractions",
			fields: fields{
				leftSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 2}, WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"e", 2}, WithID(NewRecordID("id7"))),
				}),
				leftKey: []Expression{NewVariable("left.a")},

				rightSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 11}, WithID(NewRecordID("id5"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"b", 12}, WithID(NewRecordID("id6"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"f", 12}, WithID(NewRecordID("id8"))),
				}),
				rightKey: []Expression{NewVariable("right.a")},

				joinType:       LEFT_JOIN,
				eventTimeField: "",
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"b", 2, "b", 12}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 2}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"e", 2}),
			}),

			executionCount: 4, // as above
		},
		{
			name: "outer join 1 - no event time field, no retractions",
			fields: fields{
				leftSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 2}, WithID(NewRecordID("id3"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"e", 2}, WithID(NewRecordID("id7"))),
				}),
				leftKey: []Expression{NewVariable("left.a")},

				rightSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 11}, WithID(NewRecordID("id5"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"b", 12}, WithID(NewRecordID("id6"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"f", 12}, WithID(NewRecordID("id8"))),
				}),
				rightKey: []Expression{NewVariable("right.a")},

				joinType:       OUTER_JOIN,
				eventTimeField: "",
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"b", 2, "b", 12}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 2}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"e", 2}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 11}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"b", 12}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"f", 12}),
			}),

			executionCount: 4, // as above
		},
		{
			name: "inner join 2 - no event time field, retractions",
			fields: fields{
				leftSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("id1"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2_ret")), WithUndo()),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("id1_repeated"))),
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2_ret2")), WithUndo()), // this should be ignored
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("id2_ret3")), WithUndo()), // this should be ignored as well

				}),
				leftKey: []Expression{NewVariable("left.a")},

				rightSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("id4"))),
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 11}, WithID(NewRecordID("id5"))),
				}),
				rightKey: []Expression{NewVariable("right.a")},

				joinType:       INNER_JOIN,
				eventTimeField: "",
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
			}),

			executionCount: 16, // there are basically two possibilities: either retraction for id1 comes before all records from the right
			// or not, 16 seems like a fair number of go throughs to make sure both scenarios occur
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)
			streamJoin := NewStreamJoin(tt.fields.leftSource, tt.fields.rightSource, tt.fields.leftKey, tt.fields.rightKey, stateStorage, tt.fields.eventTimeField, tt.fields.joinType)

			for i := 0; i < tt.executionCount; i++ {
				stream, _, err := GetAndStartAllShuffles(context.Background(), stateStorage, GetRawStreamID(), []Node{streamJoin}, octosql.NoVariables())
				if err != nil {
					t.Fatal(err)
				}

				want := GetTestStream(t, stateStorage, octosql.NoVariables(), tt.want)

				firstRecords, secondRecords, err := AreStreamsEqualNoOrderingWithRetractionReduction(context.Background(), stateStorage, stream[0], want, WithEqualityBasedOn(EqualityOfFieldsAndValues))
				if err != nil {
					t.Errorf("Streams aren't equal")

					if len(firstRecords) == 0 && len(secondRecords) == 0 {
						println("a")
					}

					return
				}

				println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
			}
		})
	}

}

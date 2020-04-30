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
		variables               octosql.Variables
	}

	tests := []struct {
		name           string
		fields         fields
		executionCount int
		triggerValues  []int
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
				variables:      octosql.NoVariables(),
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"b", 2, "b", 12}),
			}),

			executionCount: 16, // not much determinism, we will check trigger values from 1 to 4
			triggerValues:  []int{1, 2, 3, 4},
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
				variables:      octosql.NoVariables(),
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

			executionCount: 16, // as above
			triggerValues:  []int{1, 2, 3, 4},
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
				variables:      octosql.NoVariables(),
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

			executionCount: 16, // as above
			triggerValues:  []int{1, 2, 3, 4},
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
				variables:      octosql.NoVariables(),
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 11}),
			}),

			executionCount: 21, // there are basically two possibilities: either retraction for id1 comes before all records from the right
			triggerValues:  []int{1, 2, 3},
		},
		{
			name: "outer join 2 - multiple retractions + early retractions",
			fields: fields{
				leftSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("L1_ret1")), WithUndo()), // early retraction for (a,0)
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("L1_ret2")), WithUndo()), // count for (a,0) is -2
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("L1_1"))),                // count for (a,0) is now -1
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}, WithID(NewRecordID("L2_1"))),                // introduce new record with same key: (a,1) x1
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 0}, WithID(NewRecordID("L3_1"))),                // other key (b,0) x1
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 0}, WithID(NewRecordID("L3_2"))),                // double it (b,0) x2
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 0}, WithID(NewRecordID("L3_ret1")), WithUndo()), // retraction, so now (b,0) x1
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 0}, WithID(NewRecordID("L3_3"))),                // add again, so in the end (b,0) x2
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("L1_2"))),                // count for (a,0) is now 0
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}, WithID(NewRecordID("L1_3"))),                // count for (a,0) is now 1
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"c", 0}, WithID(NewRecordID("L4_1"))),                // no match from the left: (c,0)
					NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"c", 0}, WithID(NewRecordID("L4_2"))),                // no match from the left: (c,0) x2
				}), // in the end we have (a,0) x1; (a,1) x1; (b,0) x2; (c,0) x2
				leftKey: []Expression{NewVariable("left.a")},

				rightSource: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("R1_1"))),                // (a,10) x1
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("R1_ret1")), WithUndo()), // retraction: (a,10) x0
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"b", 10}, WithID(NewRecordID("R2"))),                  // introduce new key: (b,10) x1
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("R1_ret2")), WithUndo()), // retract again: (a,10) x(-1)
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("R1_2"))),                // (a,10) x0
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}, WithID(NewRecordID("R1_3"))),                // (a,10) x1
					NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"d", 10}, WithID(NewRecordID("R3"))),                  // no match from the right: (d,10) x1
				}), // in the end we have (a,10) x1; (b,10) x1; (d,10) x1
				rightKey: []Expression{NewVariable("right.a")},

				joinType:       OUTER_JOIN,
				eventTimeField: "",
				variables:      octosql.NoVariables(),
			},

			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 0, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"a", 1, "a", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"b", 0, "b", 10}),
				NewRecordFromSliceWithNormalize(concatFieldNames1, []interface{}{"b", 0, "b", 10}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 0}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"a", 1}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 0}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"b", 0}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"c", 0}),
				NewRecordFromSliceWithNormalize(leftFieldNames1, []interface{}{"c", 0}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"a", 10}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"b", 10}),
				NewRecordFromSliceWithNormalize(rightFieldNames1, []interface{}{"d", 10}),
			}),

			executionCount: 96, // there is a TON of possibilities, didn't even bother counting
			triggerValues:  []int{1, 2, 3, 4, 8, 16},
		},
		{
			name: "left join - the final test",
			fields: fields{
				leftSource: NewDummyNode([]*Record{}),
				leftKey:    []Expression{},

				rightSource: NewDummyNode([]*Record{}),
				rightKey:    []Expression{},

				joinType:       LEFT_JOIN,
				eventTimeField: "", // TODO: add testing for this
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeInt(0),
				}),
			},

			want: NewDummyNode([]*Record{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)
			streamJoin := NewStreamJoin(tt.fields.leftSource, tt.fields.rightSource, tt.fields.leftKey, tt.fields.rightKey, stateStorage, tt.fields.eventTimeField, tt.fields.joinType, nil)

			for i := 0; i < tt.executionCount; i++ {
				// Test different values of triggers
				triggerValue := tt.triggerValues[i%(len(tt.triggerValues))]
				trigger := NewMultiTrigger(NewWatermarkTrigger(), NewCountingTrigger(NewDummyValue(octosql.MakeInt(triggerValue))))

				streamJoin.triggerPrototype = trigger

				streamID := GetRawStreamID()

				stream, _, err := GetAndStartAllShuffles(context.Background(), stateStorage, streamID, []Node{streamJoin}, tt.fields.variables)
				if err != nil {
					t.Fatal(err)
				}

				want := GetTestStream(t, stateStorage, octosql.NoVariables(), tt.want)

				err = AreStreamsEqualNoOrderingWithRetractionReduction(context.Background(), stateStorage, stream[0], want, WithEqualityBasedOn(EqualityOfFieldsAndValues))
				if err != nil {
					t.Errorf("Streams aren't equal: %v", err)
					return
				}
			}
		})
	}

}

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
		name   string
		fields fields
		want   Node
	}{
		{
			name: "Simple inner join 1 - no event time field",
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
		},
		{
			name: "Simple left join 1 - no event time field",
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
		},
		{
			name: "Simple outer join 1 - no event time field",
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)
			streamJoin := NewStreamJoin(tt.fields.leftSource, tt.fields.rightSource, tt.fields.leftKey, tt.fields.rightKey, stateStorage, tt.fields.eventTimeField, tt.fields.joinType)

			stream, _, err := GetAndStartAllShuffles(context.Background(), stateStorage, GetRawStreamID(), []Node{streamJoin}, octosql.NoVariables())
			if err != nil {
				t.Fatal(err)
			}

			want := GetTestStream(t, stateStorage, octosql.NoVariables(), tt.want)

			err = AreStreamsEqualNoOrdering(context.Background(), stateStorage, stream[0], want, WithEqualityBasedOn(EqualityOfFieldsAndValues))
			if err != nil {
				t.Errorf("Streams aren't equal")
				return
			}
		})
	}

}

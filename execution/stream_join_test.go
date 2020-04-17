package execution

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestJoinedStream_NoRetractions_NoEventTime(t *testing.T) {
	stateStorage := GetTestStorage(t)

	defer func() {
		go stateStorage.Close()
	}()

	ctx := context.Background()

	leftFields := []octosql.VariableName{"left.a", "left.b"}
	leftSource := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{"a", 0}, WithID(NewRecordID("id1"))),
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{"a", 1}, WithID(NewRecordID("id2"))),
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{"b", 2}, WithID(NewRecordID("id3"))),
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{"e", 2}, WithID(NewRecordID("id7"))),
	})

	rightFields := []octosql.VariableName{"right.a", "right.b"}
	rightSource := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"a", 10}, WithID(NewRecordID("id4"))),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"a", 11}, WithID(NewRecordID("id5"))),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"b", 12}, WithID(NewRecordID("id6"))),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"f", 12}, WithID(NewRecordID("id8"))),
	})

	streamID := GetRawStreamID()
	outFields := []octosql.VariableName{"left.a", "left.b", "right.a", "right.b"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 0, "a", 10}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 0, "a", 11}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 1, "a", 10}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 1, "a", 11}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"b", 2, "b", 12}),
	}

	leftKey := []Expression{NewVariable("left.a")}
	rightKey := []Expression{NewVariable("right.a")}

	sj := NewStreamJoin(leftKey, rightKey, leftSource, rightSource, stateStorage, "", NewCountingTrigger(NewDummyValue(octosql.MakeInt(1))))

	tx := stateStorage.BeginTransaction()
	stream, _, err := sj.Get(storage.InjectStateTransaction(context.Background(), tx), octosql.NoVariables(), streamID)
	if err != nil {
		t.Fatal(err)
	}

	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err = AreStreamsEqualNoOrdering(ctx, stateStorage, want, stream, WithEqualityBasedOn(EqualityOfFieldsAndValues))
	if err != nil {
		t.Fatal(err)
	}
}

func TestJoinedStream_NoRetractions_WithEventTime(t *testing.T) {
	stateStorage := GetTestStorage(t)
	baseTime := time.Unix(1000000000, 0)
	outputEventTimeField := octosql.NewVariableName("output.time")

	defer func() {
		go stateStorage.Close()
	}()

	ctx := context.Background()

	leftFields := []octosql.VariableName{"left.a", "left.time"}
	leftSource := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{0, baseTime}, WithID(NewRecordID("id1")), WithEventTimeField("left.time")),
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{1, baseTime}, WithID(NewRecordID("id2")), WithEventTimeField("left.time")),
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{2, baseTime.Add(10)}, WithID(NewRecordID("id3")), WithEventTimeField("left.time")),
		NewRecordFromSliceWithNormalize(leftFields, []interface{}{3, baseTime.Add(20)}, WithID(NewRecordID("id7")), WithEventTimeField("left.time")),
	})

	rightFields := []octosql.VariableName{"right.a", "right.time"}
	rightSource := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"a", baseTime}, WithID(NewRecordID("id4")), WithEventTimeField("right.time")),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"a", baseTime}, WithID(NewRecordID("id5")), WithEventTimeField("right.time")),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"b", baseTime.Add(10)}, WithID(NewRecordID("id6")), WithEventTimeField("right.time")),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"b", baseTime.Add(30)}, WithID(NewRecordID("id8")), WithEventTimeField("right.time")),
	})

	streamID := GetRawStreamID()
	outFields := []octosql.VariableName{"left.a", "left.time", "right.a", "right.time", "output.time"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{0, baseTime, "a", baseTime, baseTime}, WithID(NewRecordID(fmt.Sprintf("%s.0", streamID.Id))), WithEventTimeField(outputEventTimeField)),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{0, baseTime, "a", baseTime, baseTime}, WithID(NewRecordID(fmt.Sprintf("%s.1", streamID.Id))), WithEventTimeField(outputEventTimeField)),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{1, baseTime, "a", baseTime, baseTime}, WithID(NewRecordID(fmt.Sprintf("%s.2", streamID.Id))), WithEventTimeField(outputEventTimeField)),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{1, baseTime, "a", baseTime, baseTime}, WithID(NewRecordID(fmt.Sprintf("%s.3", streamID.Id))), WithEventTimeField(outputEventTimeField)),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{2, baseTime.Add(10), "b", baseTime.Add(10), baseTime.Add(10)}, WithID(NewRecordID(fmt.Sprintf("%s.4", streamID.Id))), WithEventTimeField(outputEventTimeField)),
	}

	leftKey := []Expression{NewVariable("left.time")}
	rightKey := []Expression{NewVariable("right.time")}

	sj := NewStreamJoin(leftKey, rightKey, leftSource, rightSource, stateStorage, outputEventTimeField, NewCountingTrigger(NewDummyValue(octosql.MakeInt(1))))

	tx := stateStorage.BeginTransaction()
	stream, _, err := sj.Get(storage.InjectStateTransaction(context.Background(), tx), octosql.NoVariables(), streamID)
	if err != nil {
		t.Fatal(err)
	}

	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err = AreStreamsEqualNoOrdering(ctx, stateStorage, want, stream, WithEqualityBasedOn(EqualityOfFieldsAndValues, EqualityOfEventTimeField))
	if err != nil {
		t.Fatal(err)
	}
}

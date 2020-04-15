package execution

import (
	"context"
	"fmt"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestJoinedStream_Simple(t *testing.T) {
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
	})

	rightFields := []octosql.VariableName{"right.a", "right.b"}
	rightSource := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"a", 10}, WithID(NewRecordID("id4"))),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"a", 11}, WithID(NewRecordID("id5"))),
		NewRecordFromSliceWithNormalize(rightFields, []interface{}{"b", 12}, WithID(NewRecordID("id6"))),
	})

	streamID := GetRawStreamID()
	outFields := []octosql.VariableName{"left.a", "left.b", "right.a", "right.b"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 0, "a", 10}, WithID(NewRecordID(fmt.Sprintf("%s.0", streamID.Id)))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 0, "a", 11}, WithID(NewRecordID(fmt.Sprintf("%s.1", streamID.Id)))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 1, "a", 10}, WithID(NewRecordID(fmt.Sprintf("%s.2", streamID.Id)))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"a", 1, "a", 11}, WithID(NewRecordID(fmt.Sprintf("%s.3", streamID.Id)))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{"b", 2, "b", 12}, WithID(NewRecordID(fmt.Sprintf("%s.4", streamID.Id)))),
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

	err = AreStreamsEqualNoOrdering(ctx, stateStorage, want, stream)
	if err != nil {
		t.Fatal(err)
	}
}

package execution_test

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/aggregates"
	"github.com/cube2222/octosql/storage"
)

func TestGroupBy_SimpleBatch(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	ctx := context.Background()
	fields := []octosql.VariableName{"cat", "livesleft", "ownerid"}
	source := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 5, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 4, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 3, 3}),
	})

	gb := NewGroupBy(
		stateStorage,
		source,
		[]Expression{NewVariable(octosql.NewVariableName("ownerid"))},
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft"),
			octosql.NewVariableName("livesleft"),
		},
		[]AggregatePrototype{
			aggregates.AggregateTable["key"],
			aggregates.AggregateTable["avg"],
			aggregates.AggregateTable["count"],
		},
		octosql.NewVariableName(""),
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft_avg"),
			octosql.NewVariableName("livesleft_count"),
		},
		octosql.NewVariableName(""),
		NewWatermarkTrigger(),
	)

	outFields := []octosql.VariableName{"ownerid", "livesleft_avg", "livesleft_count"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 4.0, 3}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{5, 9.0, 1}),
	}

	stream := GetTestStream(t, stateStorage, octosql.NoVariables(), gb)

	tx := stateStorage.BeginTransaction()
	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err := AreStreamsEqualNoOrderingWithIDCheck(ctx, stateStorage, stream, want, WithEqualityBasedOn(EqualityOfEverythingButIDs))
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close group_by stream: %v", err)
		return
	}
	if err := want.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

func TestGroupBy_BatchWithUndos(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	ctx := context.Background()
	fields := []octosql.VariableName{"cat", "livesleft", "ownerid"}
	source := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 5, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 6, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 4, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 4, 3}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 4, 3}),
	})

	gb := NewGroupBy(
		stateStorage,
		source,
		[]Expression{NewVariable(octosql.NewVariableName("ownerid"))},
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft"),
			octosql.NewVariableName("livesleft"),
		},
		[]AggregatePrototype{
			aggregates.AggregateTable["key"],
			aggregates.AggregateTable["avg"],
			aggregates.AggregateTable["count"],
		},
		octosql.NewVariableName(""),
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft_avg"),
			octosql.NewVariableName("livesleft_count"),
		},
		octosql.NewVariableName(""),
		NewWatermarkTrigger(),
	)

	outFields := []octosql.VariableName{"ownerid", "livesleft_avg", "livesleft_count"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 5.0, 2}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 5.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{5, 9.0, 1}),
	}

	stream := GetTestStream(t, stateStorage, octosql.NoVariables(), gb)

	tx := stateStorage.BeginTransaction()
	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err := AreStreamsEqualNoOrderingWithIDCheck(ctx, stateStorage, stream, want, WithEqualityBasedOn(EqualityOfEverythingButIDs))
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close group_by stream: %v", err)
		return
	}
	if err := want.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

func TestGroupBy_WithOutputUndos(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	ctx := context.Background()
	fields := []octosql.VariableName{"cat", "livesleft", "ownerid"}
	source := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 5, 4}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 6, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 4, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 4, 3}, WithUndo()),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 4, 3}),
	})

	variables := map[octosql.VariableName]octosql.Value{
		octosql.NewVariableName("count"): octosql.MakeInt(1),
	}

	gb := NewGroupBy(
		stateStorage,
		source,
		[]Expression{NewVariable(octosql.NewVariableName("ownerid"))},
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft"),
			octosql.NewVariableName("livesleft"),
		},
		[]AggregatePrototype{
			aggregates.AggregateTable["key"],
			aggregates.AggregateTable["avg"],
			aggregates.AggregateTable["count"],
		},
		octosql.NewVariableName(""),
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft_avg"),
			octosql.NewVariableName("livesleft_count"),
		},
		octosql.NewVariableName(""),
		NewCountingTrigger(NewVariable(octosql.NewVariableName("count"))),
	)

	outFields := []octosql.VariableName{"ownerid", "livesleft_avg", "livesleft_count"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{5, 9.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}, WithUndo()),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}, WithUndo()),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 6.0, 1}, WithUndo()),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{4, 5.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 6.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 6.0, 1}, WithUndo()),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 5.0, 2}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 5.0, 2}, WithUndo()),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 6.0, 1}),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 6.0, 1}, WithUndo()),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 5.0, 2}),
	}

	stream := GetTestStream(t, stateStorage, variables, gb)

	tx := stateStorage.BeginTransaction()
	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err := AreStreamsEqualNoOrderingWithIDCheck(ctx, stateStorage, stream, want, WithEqualityBasedOn(EqualityOfEverythingButIDs))
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close group_by stream: %v", err)
		return
	}
	if err := want.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

func TestGroupBy_newRecordsNoChanges(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	ctx := context.Background()
	fields := []octosql.VariableName{"cat", "livesleft", "ownerid"}
	source := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 5, 3}),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 5, 3}),
	})

	variables := map[octosql.VariableName]octosql.Value{
		octosql.NewVariableName("count"): octosql.MakeInt(1),
	}

	gb := NewGroupBy(
		stateStorage,
		source,
		[]Expression{NewVariable(octosql.NewVariableName("ownerid"))},
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft"),
		},
		[]AggregatePrototype{
			aggregates.AggregateTable["key"],
			aggregates.AggregateTable["avg"],
		},
		octosql.NewVariableName(""),
		[]octosql.VariableName{
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft_avg"),
		},
		octosql.NewVariableName(""),
		NewCountingTrigger(NewVariable(octosql.NewVariableName("count"))),
	)

	outFields := []octosql.VariableName{"ownerid", "livesleft_avg"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{3, 5.0}),
	}

	stream := GetTestStream(t, stateStorage, variables, gb)

	tx := stateStorage.BeginTransaction()
	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err := AreStreamsEqualNoOrderingWithIDCheck(ctx, stateStorage, stream, want, WithEqualityBasedOn(EqualityOfEverythingButIDs))
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close group_by stream: %v", err)
		return
	}
	if err := want.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

func TestGroupBy_EventTimes(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	start := time.Date(2020, 7, 2, 14, 0, 0, 0, time.UTC)
	firstWindow := start
	secondWindow := start.Add(time.Minute * 10)
	thirdWindow := start.Add(time.Minute * 20)

	ctx := context.Background()
	fields := []octosql.VariableName{"cat", "livesleft", "ownerid", "t"}
	source := NewDummyNode([]*Record{
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5, firstWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 6, 4, firstWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 6, 3, firstWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 5, 3, firstWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 4, 3, firstWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5, secondWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 6, 3, secondWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 4, 3, secondWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, 5, thirdWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 5, 3, thirdWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
		NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 4, 3, thirdWindow}, WithEventTimeField(octosql.NewVariableName("t"))),
	})

	gb := NewGroupBy(
		stateStorage,
		source,
		[]Expression{
			NewVariable(octosql.NewVariableName("ownerid")),
			NewVariable(octosql.NewVariableName("t")),
		},
		[]octosql.VariableName{
			octosql.NewVariableName("t"),
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft"),
			octosql.NewVariableName("livesleft"),
		},
		[]AggregatePrototype{
			aggregates.AggregateTable["key"],
			aggregates.AggregateTable["key"],
			aggregates.AggregateTable["avg"],
			aggregates.AggregateTable["count"],
		},
		octosql.NewVariableName("t"),
		[]octosql.VariableName{
			octosql.NewVariableName("renamed_t"),
			octosql.NewVariableName("ownerid"),
			octosql.NewVariableName("livesleft_avg"),
			octosql.NewVariableName("livesleft_count"),
		},
		octosql.NewVariableName("renamed_t"),
		NewWatermarkTrigger(),
	)

	outFields := []octosql.VariableName{"renamed_t", "ownerid", "livesleft_avg", "livesleft_count"}
	expectedOutput := []*Record{
		NewRecordFromSliceWithNormalize(outFields, []interface{}{firstWindow, 5, 9.0, 1}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{firstWindow, 4, 6.0, 1}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{firstWindow, 3, 5.0, 3}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{secondWindow, 5, 9.0, 1}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{secondWindow, 3, 5.0, 2}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{thirdWindow, 5, 9.0, 1}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
		NewRecordFromSliceWithNormalize(outFields, []interface{}{thirdWindow, 3, 4.5, 2}, WithEventTimeField(octosql.NewVariableName("renamed_t"))),
	}

	stream := GetTestStream(t, stateStorage, octosql.NoVariables(), gb)

	tx := stateStorage.BeginTransaction()
	want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err := AreStreamsEqualNoOrderingWithIDCheck(ctx, stateStorage, stream, want, WithEqualityBasedOn(EqualityOfEverythingButIDs))
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close group_by stream: %v", err)
		return
	}
	if err := want.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

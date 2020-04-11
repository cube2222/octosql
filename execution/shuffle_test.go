package execution

import (
	"context"
	"log"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestName(t *testing.T) {
	ctx := context.Background()
	db := GetTestStorage(t)

	inPart0 := NewDummyNode([]*Record{
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("red"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("0.0")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("blue"),
				octosql.MakeInt(3),
			},
			WithID(NewRecordID("0.1")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("red"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("0.2")),
		),
	})
	inPart1 := NewDummyNode([]*Record{
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("green"),
				octosql.MakeInt(4),
			},
			WithID(NewRecordID("1.0")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("blue"),
				octosql.MakeInt(3),
			},
			WithID(NewRecordID("1.1")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("green"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("1.2")),
		),
	})
	inPart2 := NewDummyNode([]*Record{
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("green"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("2.0")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("blue"),
				octosql.MakeInt(3),
			},
			WithID(NewRecordID("2.1")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("red"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("2.2")),
		),
	})

	shuffle := NewShuffle(
		2,
		NewKeyHashingStrategy(octosql.NoVariables(), []Expression{NewVariable(octosql.NewVariableName("color"))}),
		[]Node{inPart0, inPart1, inPart2},
	)

	tx := db.BeginTransaction()
	ctxWithTx := storage.InjectStateTransaction(ctx, tx)

	streams, execOutputs, err := GetAndStartAllShuffles(ctxWithTx, db, tx, []Node{shuffle, shuffle}, octosql.NoVariables())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	streams = streams
	execOutputs = execOutputs

	records0, err := ReadAll(ctx, db, streams[0])
	if err != nil {
		t.Fatal(err)
	}
	records0 = records0

	log.Printf("%+v", records0)

	records1, err := ReadAll(ctx, db, streams[1])
	if err != nil {
		t.Fatal(err)
	}
	records1 = records1

	log.Printf("%+v", records1)

	t.Fatal("ok")
}

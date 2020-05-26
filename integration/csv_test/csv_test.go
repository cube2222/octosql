package csv_test

import (
	"context"
	"log"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/integration"
	"github.com/cube2222/octosql/storage"
)

func Test_CSV(t *testing.T) {
	configPath := "fixtures/config.yaml"
	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	peopleFields := []octosql.VariableName{"p.age", "p.eye_color", "p.id", "p.name"}

	type args struct {
		query string
	}
	tests := []struct {
		name    string
		args    args
		want    execution.RecordStream
		wantErr bool
	}{
		{
			name: "Get all records - SELECT *",
			args: args{
				query: "SELECT * FROM people p",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{7, "blue", 1, "Bill"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{14, "brown", 2, "Jenny"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{23, "green", 3, "Adam"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{29, "brown", 4, "Kate"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{37, "brown", 5, "Lisa"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{74, "gray", 6, "Ray"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{44, "blue", 7, "Hilary"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{27, "brown", 8, "Michael"}),
			}),
		},
		{
			name: "Get some records - FILTER",
			args: args{
				query: "SELECT * FROM people p WHERE p.age > 60 OR p.eye_color ='blue'",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{7, "blue", 1, "Bill"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{74, "gray", 6, "Ray"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{44, "blue", 7, "Hilary"}),
			}),
		},
		{
			name: "Average age by eye color - GROUP BY and ALIASES",
			args: args{
				query: "SELECT p.eye_color as color, AVG(p.age) as avg_age FROM people p GROUP BY p.eye_color",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"color", "avg_age"}, []interface{}{"blue", 25.5}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"color", "avg_age"}, []interface{}{"gray", 74.0}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"color", "avg_age"}, []interface{}{"brown", 26.75}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"color", "avg_age"}, []interface{}{"green", 23.0}),
			}),
		},
	}

	if err := tx.Commit(); err != nil {
		log.Fatal("couldn't commit transaction")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRecords, err := integration.MainCopy(tt.args.query, configPath)
			if err != nil {
				log.Fatal(err)
			}

			tx := stateStorage.BeginTransaction()
			gotStream := execution.NewInMemoryStream(storage.InjectStateTransaction(ctx, tx), gotRecords)
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrderingWithRetractionReductionAndIDChecking(ctx, stateStorage, gotStream, tt.want, execution.WithEqualityBasedOn(execution.EqualityOfEverythingButIDs)); err != nil {
				log.Fatal(err)
			}
		})
	}
}

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
			name: "get everything",
			args: args{
				query: "SELECT * FROM people p",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{7, "blue", 1, "Bill"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{14, "brown", 2, "Jenny"}),
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

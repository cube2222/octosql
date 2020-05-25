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

	peopleFields := []octosql.VariableName{"id", "name", "age", "eye_color"}

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
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{1, "Bill", 7, "blue"}),
				execution.NewRecordFromSliceWithNormalize(peopleFields, []interface{}{2, "Jenny", 14, "brown"}),
			}),
		},
	}

	if err := tx.Commit(); err != nil {
		log.Fatal("couldn't commit transaction")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := integration.MainCopy(tt.args.query, configPath)
			if err != nil {
				log.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrdering(ctx, stateStorage, rs, tt.want); err != nil {
				log.Fatal(err)
			}
		})
	}
}

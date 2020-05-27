package csv_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/integration"
	"github.com/cube2222/octosql/storage"
)

func Test_JSON(t *testing.T) {
	configPath := "../fixtures/config.yaml"
	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	defer func() {
		_ = stateStorage.Close()
		_ = os.RemoveAll("testdb")
	}()

	allFields := []octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"}

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
				query: "SELECT * FROM bikes5 b",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(allFields, []interface{}{"black", 1.0, 6.0, 2.0, 2008.0}),
				execution.NewRecordFromSliceWithNormalize(allFields, []interface{}{"blue", 4.0, 10.0, 2.0, 2010.0}),
				execution.NewRecordFromSliceWithNormalize(allFields, []interface{}{"green", 2.0, 8.0, 1.0, 2007.0}),
				execution.NewRecordFromSliceWithNormalize(allFields, []interface{}{"silver", 3.0, 8.0, 2.0, 1994.0}),
				execution.NewRecordFromSliceWithNormalize(allFields, []interface{}{"silver", 5.0, 4.0, 2.0, 2003.0}),
			}),
		},
		{
			name: "Get some records - FILTER",
			args: args{
				query: "SELECT b.id, b.color FROM bikes5 b WHERE b.year > 2000.0 * b.wheels OR b.id = 3.0",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.id", "b.color"}, []interface{}{3.0, "silver"}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.id", "b.color"}, []interface{}{2.0, "green"}),
			}),
		},
		{
			name: "Count of specific wheel numbers - GROUP BY",
			args: args{
				query: "SELECT b.wheels, COUNT(*) as count FROM bikes45 b GROUP BY b.wheels",
			},

			want: execution.NewInMemoryStream(ctx, []*execution.Record{
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{1.0, 3}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{2.0, 37}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{4.0, 1}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{6.0, 1}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{8.0, 1}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{9.0, 1}),
				execution.NewRecordFromSliceWithNormalize([]octosql.VariableName{"b.wheels", "count"}, []interface{}{12.0, 1}),
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

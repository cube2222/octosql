package parquet

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/streaming/storage"
)

type Bike struct {
	Id      int64  `parquet:"name=id, type=INT64"`
	Wheels  int64  `parquet:"name=wheels, type=INT64"`
	Year    int64  `parquet:"name=year, type=INT64"`
	OwnerId int64  `parquet:"name=ownerid, type=INT64"`
	Color   string `parquet:"name=color, type=BYTE_ARRAY"`
}

func TestParquetRecordStream_Get(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

	tests := []struct {
		name  string
		path  string
		alias string
		want  []*execution.Record
	}{
		{
			name:  "reading bikes.parquet - happy path",
			path:  "fixtures/bikes.parquet",
			alias: "b",
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"green", 1, 152849, 3, 2014},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"black", 2, 106332, 2, 1988},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"purple", 3, 99148, 2, 2009},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"orange", 4, 97521, 2, 1979},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			ds, err := NewDataSourceBuilderFactory()("test", tt.alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":      tt.path,
								"batchSize": 2,
							},
						},
					},
				},
				Storage: stateStorage,
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got := execution.GetTestStream(t, stateStorage, octosql.NoVariables(), ds, execution.GetTestStreamWithStreamID(streamId))

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(tt.want).Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), streamId)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrdering(storage.InjectStateTransaction(ctx, tx), stateStorage, want, got); err != nil {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}

			if err := got.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close parquet stream: %v", err)
				return
			}

			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}

package json

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

func TestJSONRecordStream_Get(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

	tests := []struct {
		name        string
		path        string
		arrayFormat bool
		alias       string
		want        []*execution.Record
	}{
		{
			name:        "reading bikes.json - happy path",
			path:        "fixtures/bikes.json",
			arrayFormat: false,
			alias:       "b",
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"green", 1.0, 152849.0, 3.0, 2014.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"black", 2.0, 106332.0, 2.0, 1988.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"purple", 3.0, 99148.0, 2.0, 2009.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"orange", 4.0, 97521.0, 2.0, 1979.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"yellow", 5.0, 123466.0, 3.0, 1989.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 4)),
				),
			},
		},
		{
			name:        "reading bikes.json in array format - happy path",
			path:        "fixtures/bikes_array.json",
			arrayFormat: true,
			alias:       "b",
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"green", 1.0, 152849.0, 3.0, 2014.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"black", 2.0, 106332.0, 2.0, 1988.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"purple", 3.0, 99148.0, 2.0, 2009.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"orange", 4.0, 97521.0, 2.0, 1979.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{"yellow", 5.0, 123466.0, 3.0, 1989.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 4)),
				),
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
								"path":        tt.path,
								"arrayFormat": tt.arrayFormat,
								"batchSize":   2,
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
				t.Errorf("Couldn't close json stream: %v", err)
				return
			}
			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}

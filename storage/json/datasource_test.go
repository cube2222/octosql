package json

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

func TestJSONRecordStream_Get(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		path        string
		arrayFormat bool
		alias       string
		want        execution.RecordStream
	}{
		{
			name:        "reading bikes.json - happy path",
			path:        "fixtures/bikes.json",
			arrayFormat: false,
			alias:       "b",
			want: execution.NewInMemoryStream(
				[]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"green", 1.0, 152849.0, 3.0, 2014.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"black", 2.0, 106332.0, 2.0, 1988.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"purple", 3.0, 99148.0, 2.0, 2009.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"orange", 4.0, 97521.0, 2.0, 1979.0},
					),
				},
			),
		},
		{
			name:        "reading bikes.json in array format - happy path",
			path:        "fixtures/bikes_array.json",
			arrayFormat: true,
			alias:       "b",
			want: execution.NewInMemoryStream(
				[]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"green", 1.0, 152849.0, 3.0, 2014.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"black", 2.0, 106332.0, 2.0, 1988.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"purple", 3.0, 99148.0, 2.0, 2009.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]string{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"orange", 4.0, 97521.0, 2.0, 1979.0},
					),
				},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewDataSourceBuilderFactory()("test", tt.alias).Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":        tt.path,
								"arrayFormat": tt.arrayFormat,
							},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got, err := ds.Get(ctx, octosql.NoVariables(), execution.GetRawStreamID())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			if ok, err := execution.AreStreamsEqual(context.Background(), tt.want, got); !ok {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}
		})
	}
}

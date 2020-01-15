package parquet

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type Bike struct {
	Id      int64  `parquet:"name=id, type=INT64"`
	Wheels  int64  `parquet:"name=wheels, type=INT64"`
	Year    int64  `parquet:"name=year, type=INT64"`
	OwnerId int64  `parquet:"name=ownerid, type=INT64"`
	Color   string `parquet:"name=color, type=BYTE_ARRAY"`
}

func TestParquetRecordStream_Get(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		alias string
		want  execution.RecordStream
	}{
		{
			name:  "reading bikes.parquet - happy path",
			path:  "fixtures/bikes.parquet",
			alias: "b",
			want: execution.NewInMemoryStream(
				[]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"green", 1, 152849, 3, 2014},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"black", 2, 106332, 2, 1988},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"purple", 3, 99148, 2, 2009},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"orange", 4, 97521, 2, 1979},
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
								"path": tt.path,
							},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got, err := ds.Get(context.Background(), octosql.NoVariables())
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

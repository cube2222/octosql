package json

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func TestJSONRecordStream_Next(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		alias  string
		fields []string
		want   execution.RecordStream
	}{
		{
			name:  "reading bikes.json - happy path",
			path:  "fixtures/bikes.json",
			alias: "b",
			want: execution.NewInMemoryStream(
				[]*execution.Record{
					execution.NewRecordFromSlice(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"green", 1.0, 152849.0, 3.0, 2014.0},
					),
					execution.NewRecordFromSlice(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"black", 2.0, 106332.0, 2.0, 1988.0},
					),
					execution.NewRecordFromSlice(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"purple", 3.0, 99148.0, 2.0, 2009.0},
					),
					execution.NewRecordFromSlice(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"orange", 4.0, 97521.0, 2.0, 1979.0},
					),
				},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewDataSourceBuilderFactory(tt.path)(tt.alias).Materialize(context.Background())
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got, err := ds.Get(octosql.NoVariables())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			if ok, err := execution.AreStreamsEqual(tt.want, got); !ok {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}
		})
	}
}

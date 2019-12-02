package parquet

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"context"
	"log"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)


type Bike struct {
	Id      int64   `parquet:"name=id, type=INT64"`
	Wheels  int64   `parquet:"name=wheels, type=INT64"`
	Year  int64   `parquet:"name=year, type=INT64"`
	OwnerId  int64   `parquet:"name=ownerid, type=INT64"`
	Color  string   `parquet:"name=color, type=BYTE_ARRAY"`
}

func TestParquetWrite(t *testing.T) {
	var err error
	fw, err := local.NewLocalFileWriter("fixtures/bikes.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Bike), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	bike := Bike{
		Id: 1, Wheels: 3, Color: "green", OwnerId: 152849, Year: 2014,
	}
	if err = pw.Write(bike); err != nil {
		log.Println("Write error", err)
	}
	bike = Bike{
		Id: 2, Wheels: 2, Color: "black", OwnerId: 106332, Year: 1988,
	}
	if err = pw.Write(bike); err != nil {
		log.Println("Write error", err)
	}
	bike = Bike{
		Id: 3, Wheels: 2, Color: "purple", OwnerId: 99148, Year: 2009,
	}
	if err = pw.Write(bike); err != nil {
		log.Println("Write error", err)
	}
	bike = Bike{
		Id: 4, Wheels: 2, Color: "orange", OwnerId: 97521, Year: 1979,
	}
	if err = pw.Write(bike); err != nil {
		log.Println("Write error", err)
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	_ = fw.Close()
}


func TestParquetRecordStream_Get(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		alias       string
		want        execution.RecordStream
	}{
		{
			name:        "reading bikes.parquet - happy path",
			path:        "fixtures/bikes.parquet",
			alias:       "b",
			want: execution.NewInMemoryStream(
				[]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"green", 1.0, 152849.0, 3.0, 2014.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"black", 2.0, 106332.0, 2.0, 1988.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
						[]interface{}{"purple", 3.0, 99148.0, 2.0, 2009.0},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
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

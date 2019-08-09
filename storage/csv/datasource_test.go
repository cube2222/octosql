package csv

import (
	"context"
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type csvDsc struct {
	alias string
	path  string
}

const exampleDir = "fixtures/"

var csvDbs = map[string]csvDsc{
	"people": {
		alias: "p",
		path:  exampleDir + "people.csv",
	},
	"cities": {
		alias: "c",
		path:  exampleDir + "cities.csv",
	},
	"wrongCount": {
		alias: "wc",
		path:  exampleDir + "wrongCount.csv",
	},
	"notUnique": {
		alias: "nu",
		path:  exampleDir + "notUnique.csv",
	},
	"hasHeaders": {
		alias: "hh",
		path:  exampleDir + "hasHeaders.csv",
	},
	"noHeaders": {
		alias: "nh",
		path:  exampleDir + "noHeaders.csv",
	},
}

func TestCSVDataSource_Get(t *testing.T) {
	tests := []struct {
		name    string
		csvName string
		wantErr bool
	}{
		{
			name:    "happy path",
			csvName: "cities",
			wantErr: false,
		},
		{
			name:    "not unique columns",
			csvName: "notUnique",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		ds, err := NewDataSourceBuilderFactory()("test", csvDbs[tt.csvName].alias).Materialize(context.Background(), &physical.MaterializationContext{
			Config: &config.Config{
				DataSources: []config.DataSourceConfig{
					{
						Name: "test",
						Config: map[string]interface{}{
							"path": csvDbs[tt.csvName].path,
						},
					},
				},
			},
		})
		if err != nil {
			t.Errorf("Error creating data source: %v", err)
		}

		t.Run(tt.name, func(t *testing.T) {
			_, err := ds.Get(octosql.NoVariables())
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSource.Get() error is %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCSVRecordStream_Next(t *testing.T) {
	type wanted struct {
		record *execution.Record
		error  bool
	}

	tests := []struct {
		name            string
		csvName         string
		hasColumnHeader bool
		fields          []string
		want            []wanted
	}{
		{
			name:            "reading people.csv - happy path",
			csvName:         "people",
			hasColumnHeader: true,
			fields:          []string{"name", "surname", "age", "city"},
			want: []wanted{
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name",
							"p.surname",
							"p.age",
							"p.city",
						},
						[]interface{}{"jan", "chomiak", 3, "warsaw"}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name",
							"p.surname",
							"p.age",
							"p.city",
						},
						[]interface{}{"wojtek", "kuzminski", 4, "warsaw"}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name",
							"p.surname",
							"p.age",
							"p.city",
						},
						[]interface{}{"adam", "cz", 5, "ciechanowo"}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name",
							"p.surname",
							"p.age",
							"p.city",
						},
						[]interface{}{"kuba", "m", 2, "warsaw"}),
					error: false,
				},
				{
					record: nil,
					error:  true,
				},
				{
					record: nil,
					error:  true,
				},
			},
		},
		{
			name:            "wrong numbers of columns in a row",
			csvName:         "wrongCount",
			hasColumnHeader: true,
			fields:          []string{"name", "surname"},
			want: []wanted{
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"wc.name",
							"wc.surname",
						},
						[]interface{}{"test", "test"}),
					error: false,
				},
				{
					record: nil,
					error:  true,
				},
			},
		},

		{
			name:            "file with header row",
			csvName:         "hasHeaders",
			hasColumnHeader: true,
			fields:          []string{"name", "surname"},
			want: []wanted{
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"hh.dog",
							"hh.age",
						},
						[]interface{}{"Barry", 3}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"hh.dog",
							"hh.age",
						},
						[]interface{}{"Flower", 14}),
					error: false,
				},
			},
		},

		{
			name:            "file without header row",
			csvName:         "noHeaders",
			hasColumnHeader: false,
			fields:          []string{"name", "surname"},
			want: []wanted{
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"nh.col1",
							"nh.col2",
						},
						[]interface{}{"Barry", 3}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"nh.col1",
							"nh.col2",
						},
						[]interface{}{"Flower", 14}),
					error: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewDataSourceBuilderFactory()("test", csvDbs[tt.csvName].alias).Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":      csvDbs[tt.csvName].path,
								"headerRow": tt.hasColumnHeader,
							},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}
			rs, err := ds.Get(octosql.NoVariables())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			for _, expected := range tt.want {
				got, err := rs.Next()

				if err != nil || (err != nil) != expected.error {
					if (err != nil) != expected.error {
						t.Errorf("DataSource.Next() error is %v, want %v", err, expected.error)
					}
					continue
				}

				if !reflect.DeepEqual(expected.record, got) {
					t.Errorf("DataSource.Next() is %v, want %v", expected.record, got)
				}
			}
		})
	}
}

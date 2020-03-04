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
	"peopleDot": {
		alias: "p",
		path:  exampleDir + "peopleDot.csv",
	},
	"peopleSemicolon": {
		alias: "p",
		path:  exampleDir + "peopleSemicolon.csv",
	},
}

func TestCSVDataSource_Get(t *testing.T) {
	ctx := context.Background()
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
	}

	for _, tt := range tests {
		ds, err := NewDataSourceBuilderFactory()("test", csvDbs[tt.csvName].alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
			Config: &config.Config{
				DataSources: []config.DataSourceConfig{
					{
						Name: "test",
						Config: map[string]interface{}{
							"path":      csvDbs[tt.csvName].path,
							"headerRow": true,
							"separator": ",",
						},
					},
				},
			},
		})
		if err != nil {
			t.Errorf("Error creating data source: %v", err)
		}

		t.Run(tt.name, func(t *testing.T) {
			_, err := ds.Get(ctx, octosql.NoVariables(), execution.GetRawStreamID())
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSource.Get() error is %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCSVRecordStream_Next(t *testing.T) {
	ctx := context.Background()
	type wanted struct {
		record *execution.Record
		error  bool
	}

	tests := []struct {
		name            string
		csvName         string
		hasColumnHeader bool
		separator       string
		fields          []string
		want            []wanted
	}{
		{
			name:            "reading people.csv - happy path",
			csvName:         "people",
			hasColumnHeader: true,
			separator:       ",",
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
			separator:       ",",
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
			name:            "not unique columns",
			csvName:         "notUnique",
			hasColumnHeader: true,
			separator:       ",",
			fields:          []string{"name", "name"},
			want: []wanted{
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
			separator:       ",",
			fields:          []string{"dog", "age"},
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
			separator:       ",",
			fields:          []string{"col1", "col2"},
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
		{
			name:            "reading peopleDot.csv - happy path",
			csvName:         "peopleDot",
			hasColumnHeader: true,
			separator:       ".",
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
			name:            "reading peopleSemicolon.csv - happy path",
			csvName:         "peopleSemicolon",
			hasColumnHeader: true,
			separator:       ";",
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
			name:            "reading peopleSemicolon.csv - wrong separator",
			csvName:         "people",
			hasColumnHeader: true,
			separator:       ";",
			fields:          []string{"name, surname, age, city"},
			want: []wanted{
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name, surname, age, city",
						},
						[]interface{}{"jan, chomiak, 3, warsaw"}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name, surname, age, city",
						},
						[]interface{}{"wojtek, kuzminski, 4, warsaw"}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name, surname, age, city",
						},
						[]interface{}{"adam, cz, 5, ciechanowo"}),
					error: false,
				},
				{
					record: execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"p.name, surname, age, city",
						},
						[]interface{}{"kuba, m, 2, warsaw"}),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewDataSourceBuilderFactory()("test", csvDbs[tt.csvName].alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":      csvDbs[tt.csvName].path,
								"headerRow": tt.hasColumnHeader,
								"separator": tt.separator,
							},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}
			rs, err := ds.Get(ctx, octosql.NoVariables(), execution.GetRawStreamID())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			for _, expected := range tt.want {
				got, err := rs.Next(ctx)

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

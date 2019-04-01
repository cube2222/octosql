package csv

import (
	"fmt"
	"github.com/cube2222/octosql"
	"reflect"
	"testing"
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
		path:  exampleDir + "notUnique",
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
		ds := newDataSource(csvDbs[tt.csvName].path, csvDbs[tt.csvName].alias)

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
		record map[string]interface{}
		error  bool
	}

	tests := []struct {
		name    string
		csvName string
		fields  []string
		want    []wanted
	}{
		{
			name:    "reading people.csv - happy path",
			csvName: "people",
			fields:  []string{"name", "surname", "age", "city"},
			want: []wanted{
				{
					record: map[string]interface{}{
						"name":    "jan",
						"surname": "chomiak",
						"age":     3,
						"city":    "warsaw",
					},
					error: false,
				},
				{
					record: map[string]interface{}{
						"name":    "wojtek",
						"surname": "kuzminski",
						"age":     4,
						"city":    "warsaw",
					},
					error: false,
				},
				{
					record: map[string]interface{}{
						"name":    "adam",
						"surname": "cz",
						"age":     5,
						"city":    "ciechanowo",
					},
					error: false,
				},
				{
					record: map[string]interface{}{
						"name":    "kuba",
						"surname": "m",
						"age":     2,
						"city":    "warsaw",
					},
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
			name:    "wrong numbers of columns in a row",
			csvName: "wrongCount",
			fields:  []string{"name", "surname"},
			want: []wanted{
				{
					record: map[string]interface{}{
						"name":    "andrzej",
						"surname": "lepper",
					},
					error: false,
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
			ds := newDataSource(csvDbs[tt.csvName].path, csvDbs[tt.csvName].alias)
			rs, err := ds.Get(octosql.NoVariables())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			aliasedFields := make([]string, 0)
			for i := range tt.fields {
				aliasedFields = append(aliasedFields, fmt.Sprintf("%s.%s", ds.alias, tt.fields[i]))
			}

			for _, expected := range tt.want {
				got, err := rs.Next()

				if err != nil || (err != nil) != expected.error {
					if (err != nil) != expected.error {
						t.Errorf("DataSource.Next() error is %v, want %v", err, expected.error)
					}
					continue
				}

				record := got.AsVariables()
				for i := range tt.fields {
					expectedValue := expected.record[tt.fields[i]]
					gotValue := record[octosql.VariableName(aliasedFields[i])]
					if !reflect.DeepEqual(expectedValue, gotValue) {
						t.Errorf("DataSource.Next() error is %v, want %v", expectedValue, gotValue)
					}
				}
			}
		})
	}
}

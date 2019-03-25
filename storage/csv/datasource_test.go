package csv

import (
	"fmt"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"reflect"
	"testing"
)

type csvDsc struct {
	alias string
	path  string
}

var csvDbs = map[string]csvDsc{
	"people": {
		alias: "p",
		path:  "people.csv",
	},
	"cities": {
		alias: "c",
		path:  "cities.csv",
	},
	"wrongCount": {
		alias: "wc",
		path:  "wrongCount.csv",
	},
	"notUnique": {
		alias: "nu",
		path:  "notUnique",
	},
}

/*func TestDataSource_Get(t *testing.T) {
	;
}*/

func TestRecordStream_Next(t *testing.T) {
	type wanted struct {
		record map[string]interface{}
		error  interface{}
	}

	tests := []struct {
		name    string
		csvName string
		fields  []string
		want    []wanted
	}{
		{
			name:    "SELECT * from people p",
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
					error: nil,
				},
				{
					record: map[string]interface{}{
						"name":    "wojtek",
						"surname": "kuzminski",
						"age":     4,
						"city":    "warsaw",
					},
					error: nil,
				},
				{
					record: map[string]interface{}{
						"name":    "adam",
						"surname": "cz",
						"age":     5,
						"city":    "ciechanowo",
					},
					error: nil,
				},
				{
					record: map[string]interface{}{
						"name":    "kuba",
						"surname": "m",
						"age":     2,
						"city":    "warsaw",
					},
					error: nil,
				},
				{
					record: nil,
					error:  execution.ErrEndOfStream,
				},
				{
					record: nil,
					error:  execution.ErrEndOfStream,
				},
			},
		},
	}

	for _, tt := range tests {
		ds := newDataSource(csvDbs[tt.csvName].path, csvDbs[tt.csvName].alias)
		rs, err := ds.Get(octosql.NoVariables())
		if err != nil {
			t.Errorf("DataSource.Get() error: %v", err)
		}

		aliasedFields := make([]string, 0)
		for i := range tt.fields {
			aliasedFields = append(aliasedFields, fmt.Sprintf("%s.%s", ds.alias, tt.fields[i]))
		}

		t.Run(tt.name, func(t *testing.T) {
			for _, expected := range tt.want {
				got, err := rs.Next()
				if !reflect.DeepEqual(err, expected.error) {
					t.Errorf("DataSource.Get() error is %v, want %v", err, expected.error)
				}

				record := got.AsVariables()
				for i := range tt.fields {
					expectedValue := expected.record[tt.fields[i]]
					gotValue := record[octosql.VariableName(aliasedFields[i])]
					if !reflect.DeepEqual(expectedValue, gotValue) {
						t.Errorf("DataSource.Get() error is %v, want %v", expectedValue, gotValue)
					}
				}
			}
		})
	}
}

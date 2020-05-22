package config

import (
	"reflect"
	"testing"
)

func TestReadConfig(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
		{
			name: "simple parse",
			args: args{
				path: "fixtures/example.yaml",
			},
			want: &Config{
				DataSources: []DataSourceConfig{
					{
						Name: "cities",
						Type: "csv",
						Config: map[string]interface{}{
							"path": "datasources/csv/fixtures/cities.csv",
						},
					},
					{
						Name: "bikes",
						Type: "json",
						Config: map[string]interface{}{
							"path": "datasources/json/fixtures/bikes.json",
						},
					},
					{
						Name: "users_ids",
						Type: "postgres",
						Config: map[string]interface{}{
							"address":      "localhost:5432",
							"user":         "root",
							"password":     "toor",
							"databaseName": "mydb",
							"tableName":    "users_ids",
							"primaryKeys": []interface{}{
								"name",
							},
						},
					},
					{
						Name: "users",
						Type: "redis",
						Config: map[string]interface{}{
							"address":         "localhost:6379",
							"password":        "",
							"databaseIndex":   0,
							"databaseKeyName": "key",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadConfig(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

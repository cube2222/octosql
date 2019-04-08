package redis

import (
	"fmt"
	"github.com/cube2222/octosql/physical"
	"github.com/go-redis/redis"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func TestDataSource_Get(t *testing.T) {
	hostname := "localhost"
	password := ""
	port := 6379
	dbIndex := 0
	dbKey := "key"

	type fields struct {
		hostname string
		password string
		port     int
		dbIndex  int
		dbKey    string
		filter   physical.Formula
		alias    string
		queries  map[string]map[string]interface{}
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    execution.RecordStream
		wantErr bool
	}{
		{
			name: "simple test",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "different database index",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  1,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "different database key",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    "some_other_key_name",
				filter: physical.NewPredicate(
					physical.NewVariable("r.some_other_key_name"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "simple test vol2 - or / additional queries",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewOr(
					physical.NewPredicate(
						physical.NewVariable("r.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0")),
					physical.NewPredicate(
						physical.NewVariable("r.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_1")),
				),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
					"key1": {
						"name":    "janek",
						"surname": "ch",
						"age":     "4",
						"city":    "zacisze",
					},
					"key2": {
						"name":    "kuba",
						"surname": "m",
						"age":     "2",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
					"const_1": "key1",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "4",
						"r.city":    "zacisze",
						"r.name":    "janek",
						"r.surname": "ch",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "simple redis test vol3 - and / additional variables",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable("r.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0")),
					physical.NewPredicate(
						physical.NewVariable("r.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0")),
				),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
					"const_1": "key1",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "simple redis - no filter (whole scan)",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter:   physical.NewConstant(true),
				alias:    "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
					"key1": {
						"name":    "janek",
						"surname": "ch",
						"age":     "4",
						"city":    "zacisze",
					},
					"key2": {
						"name":    "kuba",
						"surname": "m",
						"age":     "2",
						"city":    "warsaw",
					},
					"key3": {
						"name":    "adam",
						"surname": "cz",
						"age":     "1",
						"city":    "ciechanow",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "4",
						"r.city":    "zacisze",
						"r.name":    "janek",
						"r.surname": "ch",
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "1",
						"r.city":    "ciechanow",
						"r.name":    "adam",
						"r.surname": "cz",
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "2",
						"r.city":    "warsaw",
						"r.name":    "kuba",
						"r.surname": "m",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			// (("r.key" = "const_0") or ("r.key" = "const_1")) and ((("r.key" = "const_2") or ("r.key" = "const_1")) or ("r.key" = "const_0")))
			name: "complex redis test",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewAnd(
					physical.NewOr(
						physical.NewPredicate(
							physical.NewVariable("r.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0")),
						physical.NewPredicate(
							physical.NewVariable("r.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_1")),
					),
					physical.NewOr(
						physical.NewOr(
							physical.NewPredicate(
								physical.NewVariable("r.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_2")),
							physical.NewPredicate(
								physical.NewVariable("r.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_1")),
						),
						physical.NewPredicate(
							physical.NewVariable("r.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0")),
					),
				),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
					"key1": {
						"name":    "janek",
						"surname": "ch",
						"age":     "4",
						"city":    "zacisze",
					},
					"key2": {
						"name":    "kuba",
						"surname": "m",
						"age":     "2",
						"city":    "warsaw",
					},
					"key3": {
						"name":    "adam",
						"surname": "cz",
						"age":     "1",
						"city":    "ciechanow",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
					"const_1": "key1",
					"const_2": "key2",
					"const_3": "key3",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "3",
						"r.city":    "warsaw",
						"r.name":    "wojtek",
						"r.surname": "k",
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "4",
						"r.city":    "zacisze",
						"r.name":    "janek",
						"r.surname": "ch",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			// (("r.key" = "const_0") and ("const_1" = "r.key")) or ((("r.key" = "const_1") or ("r.key" = "const_2")) and ("r.key" = "const_1")))
			name: "complex redis test vol2",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewOr(
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable("r.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0")),
						physical.NewPredicate(
							physical.NewVariable("const_1"),
							physical.NewRelation("equal"),
							physical.NewVariable("r.key")),
					),
					physical.NewAnd(
						physical.NewOr(
							physical.NewPredicate(
								physical.NewVariable("r.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_1")),
							physical.NewPredicate(
								physical.NewVariable("r.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_2")),
						),
						physical.NewPredicate(
							physical.NewVariable("r.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_1")),
					),
				),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
					"key1": {
						"name":    "janek",
						"surname": "ch",
						"age":     "4",
						"city":    "zacisze",
					},
					"key2": {
						"name":    "kuba",
						"surname": "m",
						"age":     "2",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
					"const_1": "key1",
					"const_2": "key2",
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]interface{}{
						"r.age":     "4",
						"r.city":    "zacisze",
						"r.name":    "janek",
						"r.surname": "ch",
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "wrong - no variables",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias: "r",
				queries: map[string]map[string]interface{}{
					"key0": {
						"name":    "wojtek",
						"surname": "k",
						"age":     "3",
						"city":    "warsaw",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong password",
			fields: fields{
				hostname: hostname,
				password: "aaa",
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias:   "r",
				queries: map[string]map[string]interface{}{},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong hostname",
			fields: fields{
				hostname: "anyhost",
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias:   "r",
				queries: map[string]map[string]interface{}{},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong port",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     1234,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias:   "r",
				queries: map[string]map[string]interface{}{},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong dbIndex",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  20,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0")),
				alias:   "r",
				queries: map[string]map[string]interface{}{},
			},
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"const_0": "key0",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := tt.fields

			client := redis.NewClient(
				&redis.Options{
					Addr:     fmt.Sprintf("%s:%d", fields.hostname, fields.port),
					Password: fields.password,
					DB:       fields.dbIndex,
				},
			)

			_, err := client.Ping().Result()
			if err != nil {
				//t.Errorf("Couldn't connect to database")
				return
			}

			defer func() {
				for k := range fields.queries {
					_, err := client.Del(k).Result()
					if err != nil {
						t.Errorf("Couldn't delete key %s from database", k)
						return
					}
				}
			}()

			for k, v := range fields.queries {
				_, err := client.HMSet(k, v).Result()
				if err != nil {
					t.Errorf("couldn't set hash values in database")
					return
				}
			}

			dsFactory := NewDataSourceBuilderFactory(fields.hostname, fields.password, fields.port, fields.dbIndex, fields.dbKey)
			dsBuilder := dsFactory(fields.alias)
			execNode, err := dsBuilder.Executor(fields.filter, fields.alias)
			if err != nil {
				t.Errorf("%v : while executing datasource builder", err)
				return
			}

			stream, err := execNode.Get(tt.args.variables)
			if err != nil {
				//t.Errorf("Error in Get")
				return
			}

			equal, err := execution.AreStreamsEqual(stream, tt.want)
			if err != nil {
				t.Errorf("AreStreamsEqual() error: %s", err)
				return
			}

			if !equal {
				t.Errorf("ERROR: Streams are not equal")
				return
			}
		})
	}
}

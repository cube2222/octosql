package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
	"github.com/go-redis/redis"

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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key0"),
						"r.age":     octosql.MakeString("3"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("wojtek"),
						"r.surname": octosql.MakeString("k"),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.some_other_key_name", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.some_other_key_name": octosql.MakeString("key0"),
						"r.age":                 octosql.MakeString("3"),
						"r.city":                octosql.MakeString("warsaw"),
						"r.name":                octosql.MakeString("wojtek"),
						"r.surname":             octosql.MakeString("k"),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key0"),
						"r.age":     octosql.MakeString("3"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("wojtek"),
						"r.surname": octosql.MakeString("k"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key1"),
						"r.age":     octosql.MakeString("4"),
						"r.city":    octosql.MakeString("zacisze"),
						"r.name":    octosql.MakeString("janek"),
						"r.surname": octosql.MakeString("ch"),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key0"),
						"r.age":     octosql.MakeString("3"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("wojtek"),
						"r.surname": octosql.MakeString("k"),
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
				variables: map[octosql.VariableName]octosql.Value{},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key0"),
						"r.age":     octosql.MakeString("3"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("wojtek"),
						"r.surname": octosql.MakeString("k"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key1"),
						"r.age":     octosql.MakeString("4"),
						"r.city":    octosql.MakeString("zacisze"),
						"r.name":    octosql.MakeString("janek"),
						"r.surname": octosql.MakeString("ch"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key2"),
						"r.age":     octosql.MakeString("2"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("kuba"),
						"r.surname": octosql.MakeString("m"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key3"),
						"r.age":     octosql.MakeString("1"),
						"r.city":    octosql.MakeString("ciechanow"),
						"r.name":    octosql.MakeString("adam"),
						"r.surname": octosql.MakeString("cz"),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
					"const_3": octosql.MakeString("key3"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key0"),
						"r.age":     octosql.MakeString("3"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("wojtek"),
						"r.surname": octosql.MakeString("k"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key1"),
						"r.age":     octosql.MakeString("4"),
						"r.city":    octosql.MakeString("zacisze"),
						"r.name":    octosql.MakeString("janek"),
						"r.surname": octosql.MakeString("ch"),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key1"),
						"r.age":     octosql.MakeString("4"),
						"r.city":    octosql.MakeString("zacisze"),
						"r.name":    octosql.MakeString("janek"),
						"r.surname": octosql.MakeString("ch"),
					},
				),
			},
			),
			wantErr: false,
		},
		{
			name: "simple test - IN",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter: physical.NewPredicate(
					physical.NewVariable("r.key"),
					physical.NewRelation("in"),
					physical.NewTuple([]physical.Expression{
						physical.NewVariable("const_0"),
						physical.NewVariable("const_1"),
						physical.NewVariable("const_2"),
					}),
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
						"name":    "ooo",
						"surname": "aaaa",
						"age":     "2",
						"city":    "eeee",
					},
				},
			},
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
				},
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key0"),
						"r.age":     octosql.MakeString("3"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("wojtek"),
						"r.surname": octosql.MakeString("k"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key1"),
						"r.age":     octosql.MakeString("4"),
						"r.city":    octosql.MakeString("zacisze"),
						"r.name":    octosql.MakeString("janek"),
						"r.surname": octosql.MakeString("ch"),
					},
				),
				execution.NewRecord(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					map[octosql.VariableName]octosql.Value{
						"r.key":     octosql.MakeString("key2"),
						"r.age":     octosql.MakeString("2"),
						"r.city":    octosql.MakeString("warsaw"),
						"r.name":    octosql.MakeString("kuba"),
						"r.surname": octosql.MakeString("m"),
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
				variables: map[octosql.VariableName]octosql.Value{},
			},
			want:    execution.NewInMemoryStream(nil),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want:    execution.NewInMemoryStream(nil),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want:    execution.NewInMemoryStream(nil),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want:    execution.NewInMemoryStream(nil),
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
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want:    execution.NewInMemoryStream(nil),
			wantErr: true,
		},
		{
			name: "wrong db address",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     1111,
				dbIndex:  20,
				dbKey:    dbKey,
				filter:   physical.NewConstant(true),
				alias:    "r",
				queries:  map[string]map[string]interface{}{},
			},
			args: args{
				variables: map[octosql.VariableName]octosql.Value{},
			},
			want:    execution.NewInMemoryStream(nil),
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

			if tt.wantErr == false {
				_, err := client.Ping().Result()
				if err != nil {
					t.Errorf("Couldn't connect to database: %v", err)
					return
				}
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

			dsFactory := NewDataSourceBuilderFactory(fields.dbKey)
			dsBuilder := dsFactory("test", fields.alias)
			dsBuilder.Filter = physical.NewAnd(dsBuilder.Filter, fields.filter)

			execNode, err := dsBuilder.Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"address":       fmt.Sprintf("%v:%v", fields.hostname, fields.port),
								"password":      fields.password,
								"databaseIndex": fields.dbIndex,
							},
						},
					},
				},
			})
			if err != nil && !tt.wantErr {
				t.Errorf("%v : while executing datasource builder", err)
				return
			} else if err != nil {
				return
			}

			stream, err := execNode.Get(ctx, tt.args.variables)
			if err != nil && !tt.wantErr {
				t.Errorf("Error in Get: %v", err)
				return
			} else if err != nil {
				return
			}

			equal, err := execution.AreStreamsEqualNoOrdering(context.Background(), stream, tt.want)
			if err != nil && !tt.wantErr {
				t.Errorf("AreStreamsEqual() error: %s", err)
				return
			} else if err != nil {
				return
			}

			if tt.wantErr {
				t.Errorf("wanted error, but none received")
			}

			if !equal {
				t.Errorf("ERROR: Streams are not equal")
				return
			}
		})
	}
}

package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

func TestDataSource_Get(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

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
		want    []*execution.Record
		wantErr bool
	}{
		// { // Infinite loop, because error in getting key occurs (key not found)
		//	name: "test - record not in base",
		//	fields: fields{
		//		hostname: hostname,
		//		password: password,
		//		port:     port,
		//		dbIndex:  dbIndex,
		//		dbKey:    dbKey,
		//		filter: physical.NewPredicate(
		//			physical.NewVariable("r.key"),
		//			physical.NewRelation("equal"),
		//			physical.NewVariable("const_0")),
		//		alias: "r",
		//		queries: map[string]map[string]interface{}{},
		//	},
		//	args: args{
		//		variables: map[octosql.VariableName]octosql.Value{
		//			"const_0": octosql.MakeString("key0"),
		//		},
		//	},
		//	want: []*execution.Record{},
		//	wantErr: false,
		// },
		{
			name: "bigger test - no filter",
			fields: fields{
				hostname: hostname,
				password: password,
				port:     port,
				dbIndex:  dbIndex,
				dbKey:    dbKey,
				filter:   physical.NewConstant(true),
				alias:    "r",
				queries: map[string]map[string]interface{}{
					"key1":  {"value": "dummy"},
					"key2":  {"value": "dummy"},
					"key3":  {"value": "dummy"},
					"key4":  {"value": "dummy"},
					"key5":  {"value": "dummy"},
					"key6":  {"value": "dummy"},
					"key7":  {"value": "dummy"},
					"key8":  {"value": "dummy"},
					"key9":  {"value": "dummy"},
					"key10": {"value": "dummy"},
					"key11": {"value": "dummy"},
					"key12": {"value": "dummy"},
					"key13": {"value": "dummy"},
					"key14": {"value": "dummy"},
					"key15": {"value": "dummy"},
				},
			},
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"const_1":  octosql.MakeString("key1"),
					"const_2":  octosql.MakeString("key2"),
					"const_3":  octosql.MakeString("key3"),
					"const_4":  octosql.MakeString("key4"),
					"const_5":  octosql.MakeString("key5"),
					"const_6":  octosql.MakeString("key6"),
					"const_7":  octosql.MakeString("key7"),
					"const_8":  octosql.MakeString("key8"),
					"const_9":  octosql.MakeString("key9"),
					"const_10": octosql.MakeString("key10"),
					"const_11": octosql.MakeString("key11"),
					"const_12": octosql.MakeString("key12"),
					"const_13": octosql.MakeString("key13"),
					"const_14": octosql.MakeString("key14"),
					"const_15": octosql.MakeString("key15"),
				},
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key10", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key11", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key1", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key8", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key12", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 4))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key4", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 5))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key5", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 6))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key15", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 7))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key14", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 8))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key2", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 9))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key6", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 10))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key3", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 11))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key13", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 12))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key7", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 13))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.value"},
					[]interface{}{"key9", "dummy"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 14))),
			},
			wantErr: false,
		},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.some_other_key_name", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key1", "4", "zacisze", "janek", "ch"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key1", "4", "zacisze", "janek", "ch"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key3", "1", "ciechanow", "adam", "cz"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key2", "2", "warsaw", "kuba", "m"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key1", "4", "zacisze", "janek", "ch"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key1", "4", "zacisze", "janek", "ch"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
			},
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
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key0", "3", "warsaw", "wojtek", "k"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key1", "4", "zacisze", "janek", "ch"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"r.key", "r.age", "r.city", "r.name", "r.surname"},
					[]interface{}{"key2", "2", "warsaw", "kuba", "m"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
			},
			wantErr: false,
		},
		/*{
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
			want:    nil,
			wantErr: true,
		},*/
		/*{
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
			want:    []*execution.Record{},
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
			want:    []*execution.Record{},
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
			want:    []*execution.Record{},
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
			want:    []*execution.Record{},
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
			want:    []*execution.Record{},
			wantErr: true,
		},*/
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			client := redis.NewClient(
				&redis.Options{
					Addr:     fmt.Sprintf("%s:%d", tt.fields.hostname, tt.fields.port),
					Password: tt.fields.password,
					DB:       tt.fields.dbIndex,
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
				for k := range tt.fields.queries {
					_, err := client.Del(k).Result()
					if err != nil {
						t.Errorf("Couldn't delete key %s from database", k)
						return
					}
				}
			}()

			for k, v := range tt.fields.queries {
				_, err := client.HMSet(k, v).Result()
				if err != nil {
					t.Errorf("couldn't set hash values in database")
					return
				}
			}

			dsFactory := NewDataSourceBuilderFactory(tt.fields.dbKey)
			dsBuilder := dsFactory("test", tt.fields.alias)[0].(*physical.DataSourceBuilder)
			dsBuilder.Filter = physical.NewAnd(dsBuilder.Filter, tt.fields.filter)

			ds, err := dsBuilder.Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"address":       fmt.Sprintf("%v:%v", tt.fields.hostname, tt.fields.port),
								"password":      tt.fields.password,
								"databaseIndex": tt.fields.dbIndex,
								"batchSize":     2,
							},
						},
					},
				},
				Storage: stateStorage,
			})
			if err != nil && !tt.wantErr {
				t.Errorf("%v : while executing datasource builder", err)
				return
			} else if err != nil {
				return
			}

			got := execution.GetTestStream(t, stateStorage, tt.args.variables, ds)

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(tt.want).Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), streamId)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrdering(
				storage.InjectStateTransaction(ctx, tx),
				stateStorage,
				want,
				got,
				execution.WithEqualityBasedOn(
					execution.EqualityOfFieldsAndValues,
					execution.EqualityOfUndo,
				),
			); err != nil {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}

			if err := got.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close redis stream: %v", err)
				return
			}
			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}

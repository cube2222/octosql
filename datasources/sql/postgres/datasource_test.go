package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

func TestDataSource_Get(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

	host := "localhost"
	port := 5432
	user := "root"
	password := "toor"
	dbname := "mydb"

	psqlInfo, driver := template.GetDSNAndDriverName(user, password, host, dbname, port)

	db, err := sql.Open(driver, psqlInfo)
	if err != nil {
		panic("Couldn't connect to the database")
	}

	type args struct {
		tablename        string
		alias            string
		primaryKey       []octosql.VariableName
		variables        octosql.Variables
		formula          physical.Formula
		rows             [][]interface{}
		tableDescription string
	}

	tests := []struct {
		name    string
		args    args
		want    []*execution.Record
		wantErr bool
	}{
		{
			name: "SELECT * FROM animals",
			args: args{
				tablename:  "animals",
				alias:      "a",
				primaryKey: []octosql.VariableName{"name"},
				variables:  map[octosql.VariableName]octosql.Value{},
				formula:    physical.NewConstant(true),
				rows: [][]interface{}{
					{"panda", 500},
					{"human", 7000000},
					{"mammoth", 0},
					{"zebra", 5000},
				},
				tableDescription: "CREATE TABLE animals(name VARCHAR(20) PRIMARY KEY, population INTEGER);",
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"panda", 500},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"human", 7000000},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"mammoth", 0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"zebra", 5000},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3)),
				),
			},
			wantErr: false,
		},

		{
			name: "SELECT * FROM animals a WHERE a.population > 20000 - empty answer",
			args: args{
				tablename:  "animals",
				alias:      "a",
				primaryKey: []octosql.VariableName{"name"},
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeInt(20000),
				},
				formula: physical.NewPredicate(
					physical.NewVariable("a.population"),
					physical.MoreThan,
					physical.NewVariable("const_0"),
				),
				rows: [][]interface{}{
					{"panda", 500},
					{"zebra", 5000},
				},
				tableDescription: "CREATE TABLE animals(name VARCHAR(20) PRIMARY KEY, population INTEGER);",
			},
			want:    []*execution.Record{},
			wantErr: false,
		},

		{
			name: "SELECT * FROM animals a WHERE a.name = 'panda'",
			args: args{
				tablename:  "animals",
				alias:      "a",
				primaryKey: []octosql.VariableName{"name"},
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("panda"),
				},
				formula: physical.NewPredicate(
					physical.NewVariable("a.name"),
					physical.Equal,
					physical.NewVariable("const_0"),
				),
				rows: [][]interface{}{
					{"panda", 500},
					{"zebra", 5000},
					{"beaver", 5912930},
					{"duck", 291230},
				},
				tableDescription: "CREATE TABLE animals(name VARCHAR(20) PRIMARY KEY, population INTEGER);",
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"panda", 500},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
			},
			wantErr: false,
		},

		{
			name: "SELECT * FROM people p WHERE 1 <> p.id",
			args: args{
				tablename:  "people",
				alias:      "p",
				primaryKey: []octosql.VariableName{"id"},
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeInt(1),
				},
				formula: physical.NewPredicate(
					physical.NewVariable("const_0"),
					physical.NotEqual,
					physical.NewVariable("p.id"),
				),
				rows: [][]interface{}{
					{1, "Janek"},
					{2, "Kuba"},
					{3, "Wojtek"},
					{4, "Adam"},
				},
				tableDescription: "CREATE TABLE people(id INTEGER PRIMARY KEY, name VARCHAR(20));",
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{2, "Kuba"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{3, "Wojtek"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{4, "Adam"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
			wantErr: false,
		},

		{
			name: "SELECT * FROM people p WHERE 1 <> p.id AND p.name >= 'Kuba'",
			args: args{
				tablename:  "people",
				alias:      "p",
				primaryKey: []octosql.VariableName{"id"},
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeInt(1),
					"const_1": octosql.MakeString("Kuba"),
				},
				formula: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable("p.name"),
						physical.GreaterEqual,
						physical.NewVariable("const_1"),
					),
					physical.NewPredicate(
						physical.NewVariable("const_0"),
						physical.NotEqual,
						physical.NewVariable("p.id"),
					),
				),

				rows: [][]interface{}{
					{1, "Janek"},
					{2, "Kuba"},
					{3, "Wojtek"},
					{4, "Adam"},
				},
				tableDescription: "CREATE TABLE people(id INTEGER PRIMARY KEY, name VARCHAR(20));",
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{2, "Kuba"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{3, "Wojtek"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
			},
			wantErr: false,
		},

		{
			name: "SELECT * FROM people p WHERE p.name <= 'J' OR p.id = 3",
			args: args{
				tablename:  "people",
				alias:      "p",
				primaryKey: []octosql.VariableName{"id"},
				variables: map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("K"),
					"const_1": octosql.MakeInt(3),
				},
				formula: physical.NewOr(
					physical.NewPredicate(
						physical.NewVariable("p.name"),
						physical.LessEqual,
						physical.NewVariable("const_0"),
					),
					physical.NewPredicate(
						physical.NewVariable("const_1"),
						physical.Equal,
						physical.NewVariable("p.id"),
					),
				),

				rows: [][]interface{}{
					{1, "Janek"},
					{2, "Kuba"},
					{3, "Wojtek"},
					{4, "Adam"},
				},
				tableDescription: "CREATE TABLE people(id INTEGER PRIMARY KEY, name VARCHAR(20));",
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{1, "Janek"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{3, "Wojtek"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{4, "Adam"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.args
			stateStorage := storage.GetTestStorage(t)

			err := createTable(db, args.tableDescription)
			if err != nil {
				t.Errorf("Couldn't create table: %v", err)
				return
			}

			defer dropTable(db, args.tablename)

			err = insertValues(db, args.tablename, args.rows)
			if err != nil {
				t.Errorf("Couldn't insert values into table: %v", err)
				return
			}

			dsFactory := NewDataSourceBuilderFactory(args.primaryKey)
			dsBuilder := dsFactory(args.tablename, args.alias)[0].(*physical.DataSourceBuilder)
			dsBuilder.Filter = physical.NewAnd(dsBuilder.Filter, args.formula)

			execNode, err := dsBuilder.Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: args.tablename,
							Config: map[string]interface{}{
								"address":      fmt.Sprintf("%v:%v", host, port),
								"user":         user,
								"password":     password,
								"databaseName": dbname,
								"tableName":    args.tablename,
								"batchSize":    2,
							},
						},
					},
				},
				Storage: stateStorage,
			})
			if err != nil {
				t.Errorf("Couldn't get ExecutionNode: %v", err)
				return
			}

			stream := execution.GetTestStream(t, stateStorage, args.variables, execNode, execution.GetTestStreamWithStreamID(streamId))

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(tt.want).Get(storage.InjectStateTransaction(ctx, tx), args.variables, streamId)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			err = execution.AreStreamsEqualNoOrdering(storage.InjectStateTransaction(ctx, tx), stateStorage, stream, want)
			if err != nil {
				t.Errorf("Error in AreStreamsEqual(): %v", err)
				return
			}

			if err := stream.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close postgres stream: %v", err)
				return
			}
			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}

func createTable(db *sql.DB, tableDescription string) error {
	_, err := db.Exec(tableDescription)
	if err != nil {
		return errors.Wrap(err, "couldn't create table")
	}

	log.Println("created table: ", tableDescription)
	return nil
}

func insertValues(db *sql.DB, tablename string, values [][]interface{}) error {
	for i := range values {
		row := values[i]
		n := len(row)

		if n == 0 {
			continue
		}

		stringRow := sliceToString(row)

		query := fmt.Sprintf("INSERT INTO %s VALUES (%s);", tablename, strings.Join(stringRow, ", "))

		_, err := db.Exec(query)
		if err != nil {
			return errors.Wrap(err, "one of the inserts failed")
		}
	}

	return nil
}

func dropTable(db *sql.DB, tablename string) error {
	query := fmt.Sprintf("DROP TABLE %s;", tablename)
	_, err := db.Exec(query)
	if err != nil {
		return errors.Wrap(err, "couldn't drop table")
	}

	log.Println("dropped table: ", tablename)
	return nil
}

func sliceToString(values []interface{}) []string {
	var result []string
	for i := range values {
		value := values[i]
		var str string
		switch value := value.(type) {
		case string:
			str = fmt.Sprintf("'%s'", value)
		case time.Time:
			str = fmt.Sprintf("'%s'", value)
		default:
			str = fmt.Sprintf("%v", value)
		}

		result = append(result, str)
	}

	return result
}

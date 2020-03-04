package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func TestDataSource_Get(t *testing.T) {
	ctx := context.Background()
	host := "localhost"
	port := 5432
	user := "root"
	password := "toor"
	dbname := "mydb"

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic("Couldn'template connect to a database")
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
		want    *execution.InMemoryStream
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
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"human", 7000000},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"mammoth", 0},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"panda", 500},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"zebra", 5000},
				),
			},
			),
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
			want:    execution.NewInMemoryStream([]*execution.Record{}),
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
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"a.name", "a.population"},
					[]interface{}{"panda", 500},
				),
			}),
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
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{2, "Kuba"},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{3, "Wojtek"},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{4, "Adam"},
				),
			}),
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
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{2, "Kuba"},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{3, "Wojtek"},
				),
			}),
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
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{1, "Janek"},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{3, "Wojtek"},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.id", "p.name"},
					[]interface{}{4, "Adam"},
				),
			}),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.args
			err := createTable(db, args.tableDescription)
			if err != nil {
				t.Errorf("Couldn'template create table: %v", err)
				return
			}

			defer dropTable(db, args.tablename) //unhandled error

			err = insertValues(db, args.tablename, args.rows)
			if err != nil {
				t.Errorf("Couldn'template insert values into table: %v", err)
				return
			}

			dsFactory := NewDataSourceBuilderFactory(args.primaryKey)
			dsBuilder := dsFactory(args.tablename, args.alias)
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
							},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Couldn'template get ExecutionNode: %v", err)
				return
			}

			stream, err := execNode.Get(ctx, args.variables)
			if err != nil {
				t.Errorf("Couldn'template get stream: %v", err)
				return
			}

			equal, err := execution.AreStreamsEqualNoOrdering(context.Background(), stream, tt.want)
			if err != nil {
				t.Errorf("Error in AreStreamsEqual(): %v", err)
				return
			}

			if !equal != tt.wantErr {
				t.Errorf("Streams don'template match")
				return
			} else {
				return
			}
		})
	}
}

func createTable(db *sql.DB, tableDescription string) error {
	_, err := db.Exec(tableDescription)
	if err != nil {
		return errors.Wrap(err, "Couldn'template create table")
	}
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
		return errors.Wrap(err, "couldn'template drop table")
	}
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

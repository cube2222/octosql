package postgres

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	_ "github.com/lib/pq"
)

/*
	VERY IMPORTANT: READ BEFORE ADDING TEST CASES
	SQL's column names are case insensitive which means you should
	either create octosql.VariableNames via the constructor or make sure
	they are lowercase, so that they match the column names returned by psql

	When passing fieldNames to the InMemoryStream you should also make sure
	that they are aliased using the argument alias
*/

func TestDataSource_Get(t *testing.T) {
	type args struct {
		host       string
		user       string
		password   string
		dbname     string
		tablename  string
		primaryKey []octosql.VariableName
		port       int
		alias      string
		variables  octosql.Variables
		formula    physical.Formula
		queries    []string
	}
	tests := []struct {
		name    string
		args    args
		want    *execution.InMemoryStream
		wantErr bool
	}{
		{
			/*
				query: "SELECT * FROM users u WHERE u.id = 2"
			*/
			name: "simple test1",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "toor",
				dbname:     "mydb",
				tablename:  "users",
				primaryKey: []octosql.VariableName{"id"},
				port:       5432,
				alias:      "u",
				formula: physical.NewPredicate(
					physical.NewVariable("u.id"),
					physical.Equal,
					physical.NewVariable("const_0"),
				),
				variables: octosql.Variables{
					"const_0": 2,
				},
				queries: []string{
					"CREATE TABLE users(id INTEGER PRIMARY KEY, username VARCHAR(30), points INTEGER, sex VARCHAR(1));",
					"INSERT INTO users VALUES(1, 'user1', 10, 'M');",
					"INSERT INTO users VALUES(2, 'user2', 300, 'F');",
					"INSERT INTO users VALUES(3, 'user3', 2, 'M');",
				},
			},
			want: execution.NewInMemoryStream(
				[]octosql.VariableName{"u.id", "u.sex", "u.points", "u.username"},
				[]map[octosql.VariableName]interface{}{
					{
						"u.id":       2,
						"u.sex":      "F",
						"u.points":   300,
						"u.username": "user2",
					},
				},
			),
			wantErr: false,
		},

		{
			/*
				query: "SELECT * FROM animals ani WHERE ani.howMany < 1000"
			*/
			name: "simple test2",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "toor",
				dbname:     "mydb",
				tablename:  "animals",
				primaryKey: []octosql.VariableName{"name"},
				port:       5432,
				alias:      "ani",
				formula: physical.NewPredicate(
					physical.NewVariable("ani.howmany"),
					physical.LessThan,
					physical.NewVariable("const_0"),
				),
				variables: octosql.Variables{
					"const_0": 1000,
				},
				queries: []string{
					"CREATE TABLE animals(name VARCHAR(25) PRIMARY KEY, howmany BIGINT);",
					"INSERT INTO animals VALUES('lion', 50000);",
					"INSERT INTO animals VALUES('panda', 500);",
					"INSERT INTO animals VALUES('mosquito', 100000000);",
					"INSERT INTO animals VALUES('human', 7000000000);",
					"INSERT INTO animals VALUES('mammoth', 0);",
				},
			},
			want: execution.NewInMemoryStream(
				[]octosql.VariableName{"ani.name", "ani.howmany"},
				[]map[octosql.VariableName]interface{}{
					{
						"ani.name":    "panda",
						"ani.howmany": 500,
					},
					{
						"ani.name":    "mammoth",
						"ani.howmany": 0,
					},
				},
			),
			wantErr: false,
		},

		{
			/*
				query: "SELECT * FROM animals ani WHERE ani.howMany < 1000 AND ani.howMany > 600"
			*/
			name: "no answers query",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "toor",
				dbname:     "mydb",
				tablename:  "animals",
				primaryKey: []octosql.VariableName{"name"},
				port:       5432,
				alias:      "ani",
				formula: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable("ani.howmany"),
						physical.LessThan,
						physical.NewVariable("const_0"),
					),

					physical.NewPredicate(
						physical.NewVariable("ani.howmany"),
						physical.MoreThan,
						physical.NewVariable("const_1"),
					),
				),
				variables: octosql.Variables{
					"const_0": 1000,
					"const_1": 600,
				},
				queries: []string{
					"CREATE TABLE animals(name VARCHAR(25) PRIMARY KEY, howmany BIGINT);",
					"INSERT INTO animals VALUES('lion', 50000);",
					"INSERT INTO animals VALUES('panda', 500);",
					"INSERT INTO animals VALUES('mosquito', 100000000);",
					"INSERT INTO animals VALUES('human', 7000000000);",
					"INSERT INTO animals VALUES('mammoth', 0);",
				},
			},
			want: execution.NewInMemoryStream(
				[]octosql.VariableName{"ani.name", "ani.howmany"},
				[]map[octosql.VariableName]interface{}{},
			),
			wantErr: false,
		},

		{
			/*
				query: "SELECT * FROM people p WHERE p.sex <> 'M'"
			*/
			name: "simple test unequal",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "toor",
				dbname:     "mydb",
				tablename:  "people",
				primaryKey: []octosql.VariableName{"id"},
				port:       5432,
				alias:      "p",
				formula: physical.NewPredicate(
					physical.NewVariable("const_0"),
					physical.NotEqual,
					physical.NewVariable("p.sex"),
				),
				variables: octosql.Variables{
					"const_0": "M",
				},
				queries: []string{
					"CREATE TABLE people(id INTEGER PRIMARY KEY, sex CHAR);",
					"INSERT INTO people VALUES(1, 'M');",
					"INSERT INTO people VALUES(2, 'F');",
					"INSERT INTO people VALUES(3, 'F');",
					"INSERT INTO people VALUES(4, 'M');",
					"INSERT INTO people VALUES(5, 'F');",
				},
			},
			want: execution.NewInMemoryStream(
				[]octosql.VariableName{"p.id", "p.sex"},
				[]map[octosql.VariableName]interface{}{
					{
						"p.id":  2,
						"p.sex": "F",
					},
					{
						"p.id":  3,
						"p.sex": "F",
					},
					{
						"p.id":  5,
						"p.sex": "F",
					},
				},
			),
			wantErr: false,
		},

		{
			name: "invalid port",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "toor",
				dbname:     "mydb",
				tablename:  "users",
				primaryKey: []octosql.VariableName{"id"},
				port:       2, //a port that surely won't connect us to a db
				alias:      "u",
				formula:    physical.NewConstant(true),
				variables:  octosql.Variables{},
				queries:    []string{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid host",
			args: args{
				host:       "nosuchhost",
				user:       "root",
				password:   "toor",
				dbname:     "mydb",
				tablename:  "users",
				primaryKey: []octosql.VariableName{"id"},
				port:       5432,
				alias:      "u",
				formula:    physical.NewConstant(true),
				variables:  octosql.Variables{},
				queries:    []string{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid password",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "nosuchpassword",
				dbname:     "mydb",
				tablename:  "users",
				primaryKey: []octosql.VariableName{"id"},
				port:       5432,
				alias:      "u",
				formula:    physical.NewConstant(true),
				variables:  octosql.Variables{},
				queries:    []string{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid dbname",
			args: args{
				host:       "localhost",
				user:       "root",
				password:   "toor",
				dbname:     "nosuchdb",
				tablename:  "users",
				primaryKey: []octosql.VariableName{"id"},
				port:       5432,
				alias:      "u",
				formula:    physical.NewConstant(true),
				variables:  octosql.Variables{},
				queries:    []string{},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.args

			psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=disable", args.host, args.port, args.user, args.password, args.dbname)

			db, err := sql.Open("postgres", psqlInfo)

			if err != nil {
				t.Errorf("Unsuspected error in open")
				return
			}

			err = db.Ping()

			if (err != nil) != tt.wantErr {
				t.Errorf("Couldn't connect to database")
				return
			} else if err != nil {
				return
			}

			defer func() {
				_, err := db.Exec(fmt.Sprintf("DROP TABLE %s;", args.tablename))

				if err != nil {
					t.Errorf("Couldn't drop table %s", args.tablename)
					return
				}

				err = db.Close()

				if err != nil {
					t.Errorf("Couldn't close connection to db")
					return
				}
			}()
			_, err = db.Exec(args.queries[0]) //create relation

			if err != nil {
				t.Errorf("Couldnt create relation %s in database %s", args.tablename, args.dbname)
				return
			}

			for i := 1; i < len(args.queries); i++ {
				query := args.queries[i]
				_, err = db.Exec(query) //insert values

				if err != nil {
					t.Errorf("Couldn't complete query: %s. %s", query, err)
					return
				}
			}

			dsFactory := NewDataSourceBuilderFactory(args.host, args.user, args.password, args.dbname, args.tablename, args.primaryKey, args.port)
			dsbuilder := dsFactory(args.alias)

			execNode, err := dsbuilder.Executor(args.formula, args.alias)

			if err != nil {
				t.Errorf("NotEqual.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			stream, err := execNode.Get(args.variables)

			if err != nil {
				t.Errorf("Unwanted error in Get")
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

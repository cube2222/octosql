package postgres

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary: {
		physical.Equal:        {},
		physical.NotEqual:     {},
		physical.MoreThan:     {},
		physical.LessThan:     {},
		physical.GreaterEqual: {},
		physical.LessEqual:    {},
		physical.Like:         {},
	},
	physical.Secondary: {
		physical.Equal:        {},
		physical.NotEqual:     {},
		physical.MoreThan:     {},
		physical.LessThan:     {},
		physical.Like:         {},
		physical.GreaterEqual: {},
		physical.LessEqual:    {},
	},
}

type DataSource struct {
	db      *sql.DB
	stmt    *sql.Stmt
	aliases map[string]execution.Expression
	alias   string
}

func NewDataSourceBuilderFactory(host string, port int, user, password, databaseName, tableName string,
	primaryKeys []octosql.VariableName) physical.DataSourceBuilderFactory {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable", host, port, user, password, databaseName)

	return physical.NewDataSourceBuilderFactory(
		func(filter physical.Formula, alias string) (execution.Node, error) {
			db, err := sql.Open("postgres", psqlInfo)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't open connection to postgres database")
			}

			aliases := newAliases(alias)

			//create a query with placeholders to prepare a statement from a physical formula
			query := formulaToSQL(filter, aliases)
			query = fmt.Sprintf("SELECT * FROM %s %s WHERE %s", tableName, alias, query)

			stmt, err := db.Prepare(query)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't prepare db for query")
			}

			//materialize the created aliases
			execAliases, err := aliases.materializeAliases()

			if err != nil {
				return nil, errors.Wrap(err, "couldn't materialize aliases")
			}

			return &DataSource{
				stmt:    stmt,
				aliases: execAliases,
				alias:   alias,
				db:      db,
			}, nil
		},
		primaryKeys,
		availableFilters,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	host, port, err := config.GetIPAddress(dbConfig, "address", config.WithDefault([]interface{}{"localhost", 5432}))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get address")
	}
	user, err := config.GetString(dbConfig, "user")
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get user")
	}
	databaseName, err := config.GetString(dbConfig, "databaseName")
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get databaseName")
	}
	tableName, err := config.GetString(dbConfig, "tableName")
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get tableName")
	}

	primaryKeysStrings, err := config.GetStringList(dbConfig, "primaryKeys", config.WithDefault([]string{}))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get primaryKeys")
	}
	var primaryKeys []octosql.VariableName
	for _, str := range primaryKeysStrings {
		primaryKeys = append(primaryKeys, octosql.NewVariableName(str))
	}

	password, err := config.GetString(dbConfig, "password") // TODO: Change to environment.
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get password")
	}

	return NewDataSourceBuilderFactory(host, port, user, password, databaseName, tableName, primaryKeys), nil
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	values := make([]interface{}, 0)

	for i := 0; i < len(ds.aliases); i++ {
		placeholder := "$" + strconv.Itoa(i+1)
		expression, ok := ds.aliases[placeholder]
		if !ok {
			return nil, errors.Errorf("couldn't get variable name for placeholder %s", placeholder)
		}

		//since we have an execution expression, then we can evaluate it given the variables
		value, err := expression.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get actual value from variables")
		}

		values = append(values, value)
	}

	rows, err := ds.stmt.Query(values...)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't query statement")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get columns from rows")
	}

	return &RecordStream{
		rows:    rows,
		columns: columns,
		isDone:  false,
		alias:   ds.alias,
	}, nil

}

type RecordStream struct {
	rows    *sql.Rows
	columns []string
	isDone  bool
	alias   string
}

func (rs *RecordStream) Close() error {
	err := rs.rows.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying SQL rows")
	}

	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.rows.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	cols := make([]interface{}, len(rs.columns))
	colPointers := make([]interface{}, len(cols))
	for i := range cols {
		colPointers[i] = &cols[i]
	}

	if err := rs.rows.Scan(colPointers...); err != nil {
		return nil, errors.Wrap(err, "couldn't scan row")
	}

	resultMap := make(map[octosql.VariableName]interface{})

	for i, columnName := range rs.columns {
		val := colPointers[i].(*interface{})
		newName := octosql.VariableName(fmt.Sprintf("%s.%s", rs.alias, columnName))
		resultMap[newName] = val
	}

	fields := make([]octosql.VariableName, 0)

	for k := range resultMap {
		fields = append(fields, k)
	}

	resultMap, ok := execution.NormalizeType(resultMap).(map[octosql.VariableName]interface{})
	if !ok {
		return nil, errors.New("couldn't cast resultMap to map[octosql.VariableName]interface{}")
	}

	return execution.NewRecord(fields, resultMap), nil
}

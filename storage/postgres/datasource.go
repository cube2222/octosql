package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
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

func NewDataSourceBuilderFactory(primaryKeys []octosql.VariableName) physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			// Get execution configuration
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
			password, err := config.GetString(dbConfig, "password")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get password")
			}

			// Build dsn
			sb := &strings.Builder{}
			sb.WriteString(fmt.Sprintf("host=%s port=%d user=%s ", host, port, user))
			if password != "" {
				sb.WriteString(fmt.Sprintf("password=%s ", password))
			}
			sb.WriteString(fmt.Sprintf("dbname=%s sslmode=disable", databaseName))

			psqlInfo := sb.String()

			// Open the connection
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
			execAliases, err := aliases.materializeAliases(matCtx)

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
		metadata.BoundedDoesntFitInLocalStorage,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	primaryKeysStrings, err := config.GetStringList(dbConfig, "primaryKeys", config.WithDefault([]string{}))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get primaryKeys")
	}
	var primaryKeys []octosql.VariableName
	for _, str := range primaryKeysStrings {
		primaryKeys = append(primaryKeys, octosql.NewVariableName(str))
	}

	return NewDataSourceBuilderFactory(primaryKeys), nil
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables) (execution.RecordStream, error) {
	values := make([]interface{}, 0)

	for i := 0; i < len(ds.aliases); i++ {
		placeholder := "$" + strconv.Itoa(i+1)
		expression, ok := ds.aliases[placeholder]
		if !ok {
			return nil, errors.Errorf("couldn't get variable name for placeholder %s", placeholder)
		}

		//since we have an execution expression, then we can evaluate it given the variables
		value, err := expression.ExpressionValue(ctx, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get actual value from variables")
		}

		values = append(values, value)
	}

	rows, err := ds.stmt.QueryContext(ctx, values...)
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

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
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

	resultMap := make(map[octosql.VariableName]octosql.Value)

	fields := make([]octosql.VariableName, len(rs.columns))
	for i, columnName := range rs.columns {
		newName := octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, columnName))
		fields[i] = newName
		resultMap[newName] = octosql.NormalizeType(cols[i])
	}

	return execution.NewRecord(fields, resultMap), nil
}

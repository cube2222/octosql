package sqlStorages

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type SQLSourceTemplate interface {
	GetIPAddress(dbConfig map[string]interface{}) (string, int, error)
	GetDSNAndDriverName(user, password, host, dbName string, port int) (string, string)
	GetPlaceholders(alias string) PlaceholderMap
	GetAvailableFilters() map[physical.FieldType]map[physical.Relation]struct{}
}

type DataSource struct {
	db           *sql.DB
	stmt         *sql.Stmt
	placeholders []execution.Expression
	alias        string
}

func NewDataSourceBuilderFactoryFromTemplate(template SQLSourceTemplate) func([]octosql.VariableName) physical.DataSourceBuilderFactory {
	return func(primaryKeys []octosql.VariableName) physical.DataSourceBuilderFactory {
		return physical.NewDataSourceBuilderFactory(
			func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partitions int) (execution.Node, error) {
				// Get execution configuration
				host, port, err := template.GetIPAddress(dbConfig)
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

				dsn, driver := template.GetDSNAndDriverName(user, password, host, databaseName, port)

				db, err := sql.Open(driver, dsn)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't connect to the database")
				}

				placeholders := template.GetPlaceholders(alias)

				// Create a query with placeholders to prepare a statement from a physical formula
				query := FormulaToSQL(filter, placeholders)
				query = fmt.Sprintf("SELECT * FROM %s %s WHERE %s", tableName, alias, query)

				stmt, err := db.Prepare(query)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't prepare db for query")
				}

				// Materialize the created placeholders
				execAliases, err := placeholders.MaterializePlaceholders(matCtx)

				if err != nil {
					return nil, errors.Wrap(err, "couldn't materialize placeholders")
				}

				return &DataSource{
					stmt:         stmt,
					placeholders: execAliases,
					alias:        alias,
					db:           db,
				}, nil
			},
			primaryKeys,
			template.GetAvailableFilters(),
			metadata.BoundedDoesntFitInLocalStorage,
			1,
		)
	}
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	values := make([]interface{}, 0)

	for _, expression := range ds.placeholders {
		// Since we have an execution expression, then we can evaluate it given the variables
		value, err := expression.ExpressionValue(ctx, variables)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get actual value from variables")
		}

		values = append(values, value.ToRawValue())
	}

	rows, err := ds.stmt.QueryContext(ctx, values...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't query statement")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get columns from rows")
	}

	return &RecordStream{
		rows:    rows,
		columns: columns,
		isDone:  false,
		alias:   ds.alias,
	}, execution.NewExecutionOutput(execution.NewZeroWatermarkGenerator()), nil

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
		return errors.Wrap(err, "couldn't close underlying SQL rows")
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

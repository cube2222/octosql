package postgres

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
	_ "github.com/lib/pq"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary: {
		physical.Equal: {},
		physical.NotEqual: {},
		physical.MoreThan: {},
		physical.LessThan: {},
		physical.Like: {},
		physical.In: {},
	},
	physical.Secondary: {
		physical.Equal: {},
		physical.NotEqual: {},
		physical.MoreThan: {},
		physical.LessThan: {},
		physical.Like: {},
		physical.In: {},
	},
}

type DataSource struct {
	db 	  *sql.DB
	stmt *sql.Stmt
	aliases *Aliases
	alias string
}

func NewDataSourceBuilderFactory(host, user, password, dbname, tablename string,
	pkey []octosql.VariableName, port int) func (alias string) *physical.DataSourceBuilder {

		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s " +
		"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)


		return physical.NewDataSourceBuilderFactory(
		func(filter physical.Formula, alias string) (execution.Node, error) {
			db, err := sql.Open("postgres", psqlInfo)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't open connection to postgres database")
			}

			aliases := NewAliases(alias)
			query, err := FormulaToSQL(filter, aliases)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get query from formula")
			}

			query = "SELECT * FROM " + tablename + " " + alias + " WHERE " + query

			stmt, err := db.Prepare(query)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't prepare db for query")
			}

			return &DataSource{
				stmt: stmt,
				aliases: aliases,
				alias: alias,
				db:	db,
			}, nil
		},
		pkey,
		availableFilters,
	)
}


func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	values := make([]interface{}, 0)

	for i := 0; i < ds.aliases.Counter; i++ {
		placeholder := "$" + strconv.Itoa(i + 1)
		originalName, ok := ds.aliases.PlaceholderToVariable[placeholder]

		if !ok {
			return nil, errors.Errorf("couldn't get variable name for placeholder %s", placeholder)
		}

		value, err := variables.Get(originalName)

		if err != nil {
			return nil, errors.Wrap(err, "couldn't get actual value from variables")
		}

		values = append(values, value)
	}

	rows, err := ds.stmt.Query(values)

	if err != nil {
		return nil, errors.Wrap(err, "couldn't execute statement")
	}

	columns, err := rows.Columns()

	if err != nil {
		return nil, errors.Wrap(err, "couldn't get columns from rows")
	}

	return &RecordStream{
		rows: rows,
		columns: columns,
		isDone: false,
		alias: ds.alias,
	}, nil

}

type RecordStream struct {
	rows *sql.Rows
	columns []string
	isDone bool
	alias string
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
		return nil, errors.Wrap(err, "couldn't scan the row")
	}

	resultMap := make(map[octosql.VariableName]interface{})

	for i, columnName := range rs.columns {
		val := colPointers[i].(*interface{})
		newName := octosql.VariableName(fmt.Sprint("%s.%s",rs.alias, columnName))
		resultMap[newName] = val
	}

	fields := make([]octosql.VariableName, 0)

	for k := range resultMap {
		fields = append(fields, k)
	}

	return execution.NewRecord(fields, resultMap), nil
}

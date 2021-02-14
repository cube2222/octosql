package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	fields []physical.SchemaField
	table  string

	db   *sql.DB
	stmt *sql.Stmt
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	rows, err := d.stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("couldn't execute database query: %w", err)
	}

	values := make([]interface{}, len(d.fields))
	for rows.Next() {
		for i := range values {
			var x interface{}
			values[i] = &x
		}
		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("couldn't scan values: %w", err)
		}
		recordValues := make([]octosql.Value, len(values))
		for i, value := range values {
			// TODO: What if it's null?
			switch value := (*value.(*interface{})).(type) {
			case int:
				recordValues[i] = octosql.NewInt(value)
			case int64:
				recordValues[i] = octosql.NewInt(int(value))
			case bool:
				recordValues[i] = octosql.NewBoolean(value)
			case float64:
				recordValues[i] = octosql.NewFloat(value)
			case string:
				recordValues[i] = octosql.NewString(value)
			case time.Time:
				recordValues[i] = octosql.NewTime(value)
			case nil:
				recordValues[i] = octosql.NewNull()
			default:
				log.Printf("unknown postgres value type, setting null: %T, %+v", value, value)
				recordValues[i] = octosql.NewNull()
			}
		}
		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(recordValues, false)); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("couldn't close database: %w", err)
	}
	return nil
}

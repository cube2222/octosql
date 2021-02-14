package postgres

import (
	"database/sql"
	"fmt"

	"github.com/davecgh/go-spew/spew"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	fields []physical.SchemaField
	table  string

	db   *sql.DB
	stmt *sql.Stmt
}

func (d *DatasourceExecuting) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	rows, err := d.stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("couldn't execute database query: %w", err)
	}

	for rows.Next() {
		values := make([]interface{}, len(d.fields))
		for i := range values {
			switch {
			case octosql.Int.Is(d.fields[i].Type) == octosql.TypeRelationIs:

			}
		}
		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("couldn't scan values: %w", err)
		}
		spew.Dump(values)
		panic("nah")
		// var msg map[string]interface{}
		// if err := decoder.Decode(&msg); err == io.EOF {
		// 	return nil
		// } else if err != nil {
		// 	return fmt.Errorf("couldn't decode message: %w", err)
		// }
		//
		// values := make([]octosql.Value, len(d.fields))
		// for i := range values {
		// 	value := msg[d.fields[i].Name]
		// 	// TODO: What if it's null?
		// 	switch value := value.(type) {
		// 	case int:
		// 		values[i] = octosql.NewInt(value)
		// 	case bool:
		// 		values[i] = octosql.NewBoolean(value)
		// 	case float64:
		// 		values[i] = octosql.NewFloat(value)
		// 	case string:
		// 		// TODO: this should happen based on the schema only.
		// 		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		// 			values[i] = octosql.NewTime(t)
		// 		} else {
		// 			values[i] = octosql.NewString(value)
		// 		}
		// 	case time.Time:
		// 		values[i] = octosql.NewTime(value)
		// 		// TODO: Parse lists.
		// 		// TODO: Parse nested objects.
		// 	}
		// }
		//
		// if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false)); err != nil {
		// 	return fmt.Errorf("couldn't produce record: %w", err)
		// }
	}
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("couldn't close database: %w", err)
	}
	panic("nah")
}

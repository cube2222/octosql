package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	fields []physical.SchemaField
	table  string

	placeholderExprs []Expression
	db               *pgx.ConnPool
	stmt             *pgx.PreparedStatement
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	placeholderValues := make([]interface{}, len(d.placeholderExprs))
	for i := range d.placeholderExprs {
		value, err := d.placeholderExprs[i].Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate pushed-down predicate placeholder expression: %w", err)
		}
		// TODO: Use internal function for this.
		placeholderValues[i] = value.ToRawGoValue()
	}

	rows, err := d.db.QueryEx(ctx, d.stmt.SQL, nil, placeholderValues...)
	if err != nil {
		return fmt.Errorf("couldn't execute database query: %w", err)
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("couldn't get row values: %w", err)
		}
		recordValues := make([]octosql.Value, len(values))
		for i, value := range values {
			switch value := value.(type) {
			case int:
				recordValues[i] = octosql.NewInt(value)
			case int8:
				recordValues[i] = octosql.NewInt(int(value))
			case int16:
				recordValues[i] = octosql.NewInt(int(value))
			case int32:
				recordValues[i] = octosql.NewInt(int(value))
			case int64:
				recordValues[i] = octosql.NewInt(int(value))
			case uint8:
				recordValues[i] = octosql.NewInt(int(value))
			case uint16:
				recordValues[i] = octosql.NewInt(int(value))
			case uint32:
				recordValues[i] = octosql.NewInt(int(value))
			case uint64:
				recordValues[i] = octosql.NewInt(int(value))
			case bool:
				recordValues[i] = octosql.NewBoolean(value)
			case float32:
				recordValues[i] = octosql.NewFloat(float64(value))
			case float64:
				recordValues[i] = octosql.NewFloat(value)
			case string:
				recordValues[i] = octosql.NewString(value)
			case time.Time:
				recordValues[i] = octosql.NewTime(value)
			case nil:
				recordValues[i] = octosql.NewNull()
			case *pgtype.Numeric:
				recordValues[i] = octosql.NewFloat(float64(value.Int.Int64()) * math.Pow10(int(value.Exp)))
			case *pgtype.VarcharArray:
				var strings []string
				if err := value.AssignTo(&strings); err != nil {
					log.Printf("couldn't decode varchar array: %s, setting null", err)
					recordValues[i] = octosql.NewNull()
				} else {
					octoValues := make([]octosql.Value, len(strings))
					for j := range strings {
						octoValues[j] = octosql.NewString(strings[j])
					}
					recordValues[i] = octosql.NewList(octoValues)
				}
			case []byte:
				// TODO: Create new datatype byte blob.
				recordValues[i] = octosql.NewString(base64.StdEncoding.EncodeToString(value))
			default:
				log.Printf("unknown postgres value type, setting null: %T, %+v", value, value)
				recordValues[i] = octosql.NewNull()

				// TODO: Handle more types.
			}
		}
		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(recordValues, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}

package cmd

import (
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var DescribeNodeSchema = physical.NewSchema(
	[]physical.SchemaField{
		{
			Name: "name",
			Type: octosql.String,
		},
		{
			Name: "type",
			Type: octosql.String,
		},
		{
			Name: "time_field",
			Type: octosql.Boolean,
		},
	},
	-1,
)

type DescribeNode struct {
	Schema physical.Schema
}

func (d *DescribeNode) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for i, field := range d.Schema.Fields {
		if err := produce(
			ProduceFromExecutionContext(ctx),
			// TODO: Fixme.
			NewRecordBatch([][]octosql.Value{
				{
					octosql.NewString(field.Name),
					octosql.NewString(field.Type.String()),
					octosql.NewBoolean(i == d.Schema.TimeField),
				},
			}, []bool{false}, []time.Time{{}}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}

package docs

import (
	"context"
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type functionsPhysical struct {
}

func (i *functionsPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &functionsExecuting{
		fields: schema.Fields,
	}, nil
}

func (i *functionsPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type functionsExecuting struct {
	fields []physical.SchemaField
}

func (d *functionsExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	fs := sortedMapNameAndDetails(functions.FunctionMap())

	output := make([][]octosql.Value, 0)
	for _, f := range fs {
		if f.Details.Description == "" {
			continue
		}
		row := make([]octosql.Value, len(d.fields))
		for i, field := range d.fields {
			switch field.Name {
			case "name":
				row[i] = octosql.NewString(f.Name)
			case "description":
				row[i] = octosql.NewString(f.Details.Description)
			}
		}
		output = append(output, row)
	}

	for i := range output {
		if err := produce(
			ProduceFromExecutionContext(ctx),
			NewRecordBatch(output[i], false, time.Time{}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}

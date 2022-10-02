package docs

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql/aggregates"
	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type aggregatesPhysical struct {
}

func (i *aggregatesPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &aggregatesExecuting{
		fields: schema.Fields,
	}, nil
}

func (i *aggregatesPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type aggregatesExecuting struct {
	fields []physical.SchemaField
}

func (d *aggregatesExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	fs := sortedMapNameAndDetails(aggregates.Aggregates)

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

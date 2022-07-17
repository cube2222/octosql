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

type aggregateSignaturesPhysical struct {
}

func (i *aggregateSignaturesPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &aggregateSignaturesExecuting{
		fields: schema.Fields,
	}, nil
}

func (i *aggregateSignaturesPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type aggregateSignaturesExecuting struct {
	fields []physical.SchemaField
}

func (d *aggregateSignaturesExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	fs := sortedMapNameAndDetails(aggregates.Aggregates)

	output := make([][]octosql.Value, 0)
	for _, f := range fs {
		if f.Details.Description == "" {
			continue
		}
		for _, descriptor := range f.Details.Descriptors {
			row := make([]octosql.Value, len(d.fields))
			for i, field := range d.fields {
				switch field.Name {
				case "name":
					row[i] = octosql.NewString(f.Name)
				case "argument_type":
					row[i] = octosql.NewString(descriptor.ArgumentType.String())
				case "output_type":
					row[i] = octosql.NewString(descriptor.OutputType.String())
				case "simple_signature":
					row[i] = octosql.NewBoolean(descriptor.TypeFn == nil)
				}
			}
			output = append(output, row)
		}
	}

	for i := range output {
		if err := produce(
			ProduceFromExecutionContext(ctx),
			NewRecord(output[i], false, time.Time{}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}

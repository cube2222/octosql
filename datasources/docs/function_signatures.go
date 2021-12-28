package docs

import (
	"context"
	"fmt"
	"sort"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type functionSignaturesPhysical struct {
}

func (i *functionSignaturesPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &functionSignaturesExecuting{
		fields: schema.Fields,
	}, nil
}

func (i *functionSignaturesPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type functionSignaturesExecuting struct {
	fields []physical.SchemaField
}

func (d *functionSignaturesExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	fs := functions.FunctionMap()

	output := make([][]octosql.Value, 0)
	for name, details := range fs {
		if details.Description == "" {
			continue
		}
		for _, descriptor := range details.Descriptors {
			parts := make([]octosql.Value, len(descriptor.ArgumentTypes))
			for i := range descriptor.ArgumentTypes {
				parts[i] = octosql.NewString(descriptor.ArgumentTypes[i].String())
			}
			output = append(output, []octosql.Value{octosql.NewString(name), octosql.NewList(parts), octosql.NewString(descriptor.OutputType.String()), octosql.NewBoolean(descriptor.Strict), octosql.NewBoolean(descriptor.TypeFn == nil)})
		}
	}
	sort.Slice(output, func(i, j int) bool {
		return octosql.NewList(output[i]).Compare(octosql.NewList(output[j])) == -1
	})

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

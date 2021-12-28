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
	fs := functions.FunctionMap()

	output := make([][]octosql.Value, 0)
	for name, details := range fs {
		if details.Description == "" {
			continue
		}
		output = append(output, []octosql.Value{octosql.NewString(name), octosql.NewString(details.Description)})
	}
	sort.Slice(output, func(i, j int) bool {
		return output[i][0].Str < output[j][0].Str
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

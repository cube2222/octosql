package plugins

import (
	"context"
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/repository"
)

type repositoriesPhysical struct {
	repositories []repository.Repository
}

func (i *repositoriesPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &repositoriesExecuting{
		repositories: i.repositories,
		fields:       schema.Fields,
	}, nil
}

func (i *repositoriesPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type repositoriesExecuting struct {
	repositories []repository.Repository
	fields       []physical.SchemaField
}

func (d *repositoriesExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for _, repo := range d.repositories {
		values := make([]octosql.Value, len(d.fields))
		for i, field := range d.fields {
			switch field.Name {
			case "slug":
				values[i] = octosql.NewString(repo.Slug)
			case "name":
				values[i] = octosql.NewString(repo.Name)
			case "description":
				values[i] = octosql.NewString(repo.Description)
			}
		}

		if err := produce(
			ProduceFromExecutionContext(ctx),
			NewRecordBatch(values, false, time.Time{}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}

package postgres

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type impl struct {
	schema physical.Schema
}

func (i *impl) Schema() (physical.Schema, error) {
	return i.schema, nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment) (execution.Node, error) {
	panic("implement me")
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, pushedDown []physical.Expression, changed bool) {
	// TODO: Implement predicate pushdown for postgres
	return newPredicates, pushedDownPredicates, false
}

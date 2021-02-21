package slicedb

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type impl struct {
	schema physical.Schema
	values interface{}
}

func (impl *impl) Schema() (physical.Schema, error) {
	return impl.schema, nil
}

func (impl *impl) Materialize(ctx context.Context, env physical.Environment, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		values: impl.values,
	}, nil
}

func (impl *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, newPushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

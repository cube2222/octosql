package slicedb

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type impl struct {
	schema physical.Schema
	table  string
}

func (impl *impl) Schema() (physical.Schema, error) {
	return impl.schema, nil
}

func (impl *impl) Materialize(ctx context.Context, env physical.Environment, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	panic("implement me")
}

func (impl *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, newPushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

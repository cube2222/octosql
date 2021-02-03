package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type Filter struct {
	predicate Expression
	source    Node
}

func NewFilter(predicate Expression, child Node) *Filter {
	return &Filter{predicate: predicate, source: child}
}

func (node *Filter) Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error) {
	panic("implement me")
}

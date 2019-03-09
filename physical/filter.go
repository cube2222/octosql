package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

// TODO: Here's the place for functions which make optimizations based on the formula possible
type Filter struct {
	formula Formula
	source  Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{formula: formula, source: child}
}

func (node *Filter) Materialize(ctx context.Context) execution.Node {
	return execution.NewFilter(node.formula.Materialize(ctx), node.source.Materialize(ctx))
}

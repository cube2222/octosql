package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

// TODO: Here's the place (in this package) for functions which make optimizations based on the formula possible
type Filter struct {
	Formula Formula
	Source  Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{Formula: formula, Source: child}
}

func (node *Filter) Materialize(ctx context.Context) execution.Node {
	return execution.NewFilter(node.Formula.Materialize(ctx), node.Source.Materialize(ctx))
}

package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

// TODO: Here's the place (in this package) for functions which make optimizations based on the formula possible
type Filter struct {
	Formula Formula
	Source  Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{Formula: formula, Source: child}
}

func (node *Filter) Materialize(ctx context.Context) (execution.Node, error) {
	materializedFormula, err := node.Formula.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize formula")
	}
	materializedSource, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source")
	}
	return execution.NewFilter(materializedFormula, materializedSource), nil
}

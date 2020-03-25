package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Filter struct {
	formula Formula
	source  Node
}

func (filter *Filter) Visualize() *graph.Node {
	n := graph.NewNode("Filter")
	if filter.formula != nil {
		n.AddChild("formula", filter.formula.Visualize())
	}
	if filter.source != nil {
		n.AddChild("source", filter.source.Visualize())
	}
	return n
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{formula: formula, source: child}
}

func (node *Filter) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	formula, formulaVariables, err := node.formula.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for formula")
	}
	child, childVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for filter source node")
	}

	variables, err := childVariables.MergeWith(formulaVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for filter source")
	}

	return physical.NewFilter(formula, child), variables, nil
}

package logical

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
)

type Filter struct {
	formula Formula
	source  Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{formula: formula, source: child}
}

func (node *Filter) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	formula, formulaVariables, err := node.formula.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for formula")
	}
	sourceNodes, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for filter source node")
	}

	variables, err := sourceVariables.MergeWith(formulaVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for filter source")
	}

	outputNodes := make([]physical.Node, len(sourceNodes))
	for i := range outputNodes {
		outputNodes[i] = physical.NewFilter(formula, sourceNodes[i])
	}

	return outputNodes, variables, nil
}

func (node *Filter) Visualize() *graph.Node {
	n := graph.NewNode("Filter")
	if node.formula != nil {
		n.AddChild("formula", node.formula.Visualize())
	}
	if node.source != nil {
		n.AddChild("source", node.source.Visualize())
	}
	return n
}

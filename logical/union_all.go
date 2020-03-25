package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type UnionAll struct {
	first, second Node
}

func (ua *UnionAll) Visualize() *graph.Node {
	n := graph.NewNode("Union All")
	if ua.first != nil {
		n.AddChild("first", ua.first.Visualize())
	}
	if ua.second != nil {
		n.AddChild("second", ua.second.Visualize())
	}
	return n
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()
	firstNode, firstVariables, err := node.first.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for first node")
	}
	variables, err = variables.MergeWith(firstVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get first node variables")
	}

	secondNode, secondVariables, err := node.second.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for second node")
	}
	variables, err = variables.MergeWith(secondVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get second node variables")
	}

	return physical.NewUnionAll(firstNode, secondNode), variables, nil
}

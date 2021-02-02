package logical

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()
	leftNodes, firstVariables, err := node.first.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for left nodes")
	}
	variables, err = variables.MergeWith(firstVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get first node variables")
	}

	rightNodes, secondVariables, err := node.second.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for right nodes")
	}
	variables, err = variables.MergeWith(secondVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get second node variables")
	}

	for partition := range leftNodes {
		leftNodes[partition] = physical.NewNextShuffleMetadataChange("_union_left", partition, leftNodes[partition])
	}
	for partition := range rightNodes {
		rightNodes[partition] = physical.NewNextShuffleMetadataChange("_union_right", partition, rightNodes[partition])
	}

	return append(leftNodes, rightNodes...), variables, nil
}

func (node *UnionAll) Visualize() *graph.Node {
	n := graph.NewNode("Union All")
	if node.first != nil {
		n.AddChild("first", node.first.Visualize())
	}
	if node.second != nil {
		n.AddChild("second", node.second.Visualize())
	}
	return n
}

package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
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

	return append(leftNodes, rightNodes...), variables, nil
}

package logical

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

func NewOffset(data Node, expr Expression) Node {
	return &Offset{data: data, offsetExpr: expr}
}

func (node *Offset) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	sourceNodes, variables, err := node.data.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for source nodes")
	}

	offsetExpr, offsetVariables, err := node.offsetExpr.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for offset expression")
	}
	variables, err = variables.MergeWith(offsetVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get offset node variables")
	}

	// Offset operates on a single, joined stream.
	outNodes := physical.NewShuffle(1, physical.NewConstantStrategy(0), sourceNodes)

	return []physical.Node{physical.NewOffset(outNodes[0], offsetExpr)}, variables, nil
}

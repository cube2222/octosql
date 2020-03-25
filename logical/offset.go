package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

func (offset *Offset) Visualize() *graph.Node {
	n := graph.NewNode("Offset")
	if offset.data != nil {
		n.AddChild("data", offset.data.Visualize())
	}
	if offset.offsetExpr != nil {
		n.AddChild("offset", offset.offsetExpr.Visualize())
	}
	return n
}

func NewOffset(data Node, expr Expression) Node {
	return &Offset{data: data, offsetExpr: expr}
}

func (node *Offset) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	dataNode, variables, err := node.data.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for data node")
	}

	offsetExpr, offsetVariables, err := node.offsetExpr.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for offset expression")
	}
	variables, err = variables.MergeWith(offsetVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get offset node variables")
	}

	return physical.NewOffset(dataNode, offsetExpr), variables, nil
}

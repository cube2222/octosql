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

func (node *Offset) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	if node.data == nil {
		return nil, nil, errors.New("Offset has a nil node underneath")
	}

	dataNode, variables, err := node.data.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for data node")
	}

	if node.offsetExpr == nil {
		return dataNode, variables, nil
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

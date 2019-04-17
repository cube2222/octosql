package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

func NewOffset(data Node, expr Expression) *Offset {
	return &Offset{data: data, offsetExpr: expr}
}

func (node *Offset) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Offset{
		data:       node.data.Transform(ctx, transformers),
		offsetExpr: node.offsetExpr.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Offset) Materialize(ctx context.Context) (execution.Node, error) {
	if node.data == nil || node.offsetExpr == nil {
		return nil, errors.New("offset has an incorrect nil field")
	}

	dataNode, err := node.data.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize data node")
	}

	offsetExpr, err := node.offsetExpr.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize offset expression")
	}

	return execution.NewOffset(dataNode, offsetExpr), nil
}

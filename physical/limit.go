package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Limit struct {
	data   Node
	limit  Expression
	offset Expression
}

func NewLimit(data Node, limit, offset Expression) *Limit {
	return &Limit{data: data, limit: limit, offset: offset}
}

func (node *Limit) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Limit{
		data:   node.data.Transform(ctx, transformers),
		limit:  node.limit.Transform(ctx, transformers),
		offset: node.offset.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Limit) Materialize(ctx context.Context) (execution.Node, error) {
	var limitExpr, offsetExpr execution.Expression = nil, nil

	dataNode, err := node.data.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize data node")
	}

	if node.limit != nil {
		limitExpr, err = node.limit.Materialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize limit expression")
		}
	}

	if node.offset != nil {
		offsetExpr, err = node.offset.Materialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize offset expression")
		}
	}

	return execution.NewLimit(dataNode, limitExpr, offsetExpr), nil
}

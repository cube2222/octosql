package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Limit struct {
	data   Node
	limit  Node
	offset Node
}

func NewLimit(data, limit, offset Node) *Limit {
	return &Limit{data: data, limit: limit, offset: offset}
}

func (node *Limit) Transform(ctx context.Context, transformers *Transformers) Node {
	var limitNode, offsetNode Node = nil, nil
	if node.limit != nil {
		limitNode = node.limit.Transform(ctx, transformers)
	}
	if node.limit != nil {
		offsetNode = node.offset.Transform(ctx, transformers)
	}

	var transformed Node = &Limit{
		data:   node.data.Transform(ctx, transformers),
		limit:  limitNode,
		offset: offsetNode,
	}
	if transformers.NodeT != nil {
		// I really hope it handles nils well
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Limit) Materialize(ctx context.Context) (execution.Node, error) {
	var limitNode, offsetNode execution.Node = nil, nil

	dataNode, err := node.data.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize data node")
	}

	if node.limit != nil {
		limitNode, err = node.limit.Materialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize limit node")
		}
	}

	if node.offset != nil {
		offsetNode, err = node.offset.Materialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize offset node")
		}
	}

	return execution.NewLimit(dataNode, limitNode, offsetNode), nil
}

package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(left, right Node) *UnionAll {
	return &UnionAll{first: left, second: right}
}

func (node *UnionAll) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &UnionAll{
		first:  node.first.Transform(ctx, transformers),
		second: node.second.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *UnionAll) Materialize(ctx context.Context) (execution.Node, error) {
	firstNode, err := node.first.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize first node")
	}
	secondNode, err := node.second.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize second node")
	}

	return execution.NewUnionAll(firstNode, secondNode), nil
}

package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type UnionAll struct {
	First, Second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{First: first, Second: second}
}

func (node *UnionAll) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &UnionAll{
		First:  node.First.Transform(ctx, transformers),
		Second: node.Second.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *UnionAll) Materialize(ctx context.Context) (execution.Node, error) {
	firstNode, err := node.First.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize first node")
	}
	secondNode, err := node.Second.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize second node")
	}

	return execution.NewUnionAll(firstNode, secondNode), nil
}

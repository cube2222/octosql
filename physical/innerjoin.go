package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type InnerJoin struct {
	Source Node
	Joined Node
}

func NewInnerJoin(source Node, joined Node) *InnerJoin {
	return &InnerJoin{Source: source, Joined: joined}
}

func (node *InnerJoin) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &InnerJoin{
		Source: node.Source.Transform(ctx, transformers),
		Joined: node.Joined.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *InnerJoin) Materialize(ctx context.Context) (execution.Node, error) {
	materializedSource, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.Joined.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	return execution.NewInnerJoin(materializedSource, materializedJoined), nil
}

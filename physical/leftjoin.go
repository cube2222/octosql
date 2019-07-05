package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type LeftJoin struct {
	Source Node
	Joined Node
}

func NewLeftJoin(source Node, joined Node) *LeftJoin {
	return &LeftJoin{Source: source, Joined: joined}
}

func (node *LeftJoin) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &LeftJoin{
		Source: node.Source.Transform(ctx, transformers),
		Joined: node.Joined.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *LeftJoin) Materialize(ctx context.Context) (execution.Node, error) {
	materializedSource, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.Joined.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	return execution.NewLeftJoin(materializedSource, materializedJoined), nil
}

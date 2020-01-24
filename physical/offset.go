package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type Offset struct {
	Source     Node
	OffsetExpr Expression
}

func NewOffset(data Node, expr Expression) *Offset {
	return &Offset{Source: data, OffsetExpr: expr}
}

func (node *Offset) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Offset{
		Source:     node.Source.Transform(ctx, transformers),
		OffsetExpr: node.OffsetExpr.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Offset) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	dataNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	offsetExpr, err := node.OffsetExpr.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize offset expression")
	}

	return execution.NewOffset(dataNode, offsetExpr), nil
}

func (node *Offset) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(node.Source.Metadata().Cardinality(), node.Source.Metadata().EventTimeField())
}

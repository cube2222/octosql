package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type Limit struct {
	Source    Node
	LimitExpr Expression
}

func NewLimit(data Node, expr Expression) *Limit {
	return &Limit{Source: data, LimitExpr: expr}
}

func (node *Limit) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Limit{
		Source:    node.Source.Transform(ctx, transformers),
		LimitExpr: node.LimitExpr.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Limit) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	dataNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	limitExpr, err := node.LimitExpr.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize limit expression")
	}

	return execution.NewLimit(dataNode, limitExpr), nil
}

func (node *Limit) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(metadata.BoundedFitsInLocalStorage, node.Source.Metadata().EventTimeField(), node.Source.Metadata().Namespace())
}

func (node *Limit) Visualize() *graph.Node {
	n := graph.NewNode("Limit")
	n.AddChild("limit", node.LimitExpr.Visualize())
	n.AddChild("source", node.Source.Visualize())
	return n
}

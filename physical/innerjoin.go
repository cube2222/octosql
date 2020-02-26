package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
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

func (node *InnerJoin) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	prefetchCount, err := config.GetInt(matCtx.Config.Execution, "lookupJoinPrefetchCount", config.WithDefault(32))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get lookupJoinPrefetchCount configuration")
	}

	materializedSource, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.Joined.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	return execution.NewInnerJoin(prefetchCount, materializedSource, materializedJoined), nil
}

func (node *InnerJoin) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(metadata.CombineCardinalities(node.Source.Metadata().Cardinality(), node.Joined.Metadata().Cardinality()), octosql.NewVariableName(""))
}

func (node *InnerJoin) Visualize() *graph.Node {
	n := graph.NewNode("Inner Join")
	n.AddChild("source", node.Source.Visualize())
	n.AddChild("joined", node.Joined.Visualize())
	return n
}

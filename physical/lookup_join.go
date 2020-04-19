package physical

import (
	"context"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type LookupJoin struct {
	source, joined Node
	isLeftJoin     bool
}

func NewLookupJoin(source, joined Node, isLeftJoin bool) *LookupJoin {
	return &LookupJoin{
		source:     source,
		joined:     joined,
		isLeftJoin: isLeftJoin,
	}
}

func (node *LookupJoin) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &LookupJoin{
		source:     node.source.Transform(ctx, transformers),
		joined:     node.joined.Transform(ctx, transformers),
		isLeftJoin: node.isLeftJoin,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *LookupJoin) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	prefetchCount, err := config.GetInt(matCtx.Config.Execution, "lookupJoinPrefetchCount", config.WithDefault(128))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get lookupJoinPrefetchCount configuration")
	}

	materializedSource, err := node.source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.joined.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	return execution.NewLookupJoin(prefetchCount, matCtx.Storage, materializedSource, materializedJoined, true), nil
}

func (node *LookupJoin) Metadata() *metadata.NodeMetadata {
	sourceMetadata := node.source.Metadata()
	joinedMetadata := node.joined.Metadata()
	cardinality := metadata.CombineCardinalities(sourceMetadata.Cardinality(), joinedMetadata.Cardinality())

	sourceNamespace := sourceMetadata.Namespace()
	sourceNamespace.MergeWith(joinedMetadata.Namespace())

	return metadata.NewNodeMetadata(cardinality, sourceMetadata.EventTimeField(), sourceNamespace)
}

func (node *LookupJoin) Visualize() *graph.Node {
	n := graph.NewNode("Lookup Join")
	n.AddChild("source", node.source.Visualize())
	n.AddChild("joined", node.joined.Visualize())
	return n
}

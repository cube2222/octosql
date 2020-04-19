package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type StreamJoin struct {
	source         Node
	joined         Node
	sourceKey      []Expression
	joinedKey      []Expression
	eventTimeField octosql.VariableName
	isLeftJoin     bool
}

func NewStreamJoin(source, joined Node, sourceKey, joinedKey []Expression, eventTimeField octosql.VariableName, isLeftJoin bool) *StreamJoin {
	return &StreamJoin{
		source:         source,
		joined:         joined,
		sourceKey:      sourceKey,
		joinedKey:      joinedKey,
		eventTimeField: eventTimeField,
		isLeftJoin:     isLeftJoin,
	}
}

func (node *StreamJoin) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &StreamJoin{
		source:         node.source.Transform(ctx, transformers),
		joined:         node.joined.Transform(ctx, transformers),
		sourceKey:      node.sourceKey,
		joinedKey:      node.joinedKey,
		eventTimeField: node.eventTimeField,
		isLeftJoin:     node.isLeftJoin,
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *StreamJoin) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	materializedSource, err := node.source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.joined.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	materializedSourceKey := make([]execution.Expression, len(node.sourceKey))
	materializedJoinedKey := make([]execution.Expression, len(node.joinedKey))

	for i := range node.sourceKey {
		materializedSource, err := node.sourceKey[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize source key expression with index %v", i)
		}

		materializedSourceKey[i] = materializedSource

		materializedJoined, err := node.joinedKey[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize joined key expression with index %v", i)
		}

		materializedJoinedKey[i] = materializedJoined
	}

	return execution.NewStreamJoin(materializedSource, materializedJoined, materializedSourceKey, materializedJoinedKey, matCtx.Storage, node.eventTimeField, node.isLeftJoin), nil
}

func (node *StreamJoin) Metadata() *metadata.NodeMetadata {
	sourceMetadata := node.source.Metadata()
	joinedMetadata := node.joined.Metadata()
	cardinality := metadata.CombineCardinalities(sourceMetadata.Cardinality(), joinedMetadata.Cardinality())

	sourceNamespace := sourceMetadata.Namespace()
	sourceNamespace.MergeWith(joinedMetadata.Namespace())

	return metadata.NewNodeMetadata(cardinality, node.eventTimeField, sourceNamespace)
}

func (node *StreamJoin) Visualize() *graph.Node {
	n := graph.NewNode("Stream Join")
	n.AddChild("source", node.source.Visualize())
	n.AddChild("joined", node.joined.Visualize())
	return n
}

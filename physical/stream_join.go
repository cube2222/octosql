package physical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type StreamJoin struct {
	Source         Node
	Joined         Node
	SourceKey      []Expression
	JoinedKey      []Expression
	EventTimeField octosql.VariableName
	JoinType       execution.JoinType
	Triggers       []Trigger
}

func NewStreamJoin(source, joined Node, sourceKey, joinedKey []Expression, eventTimeField octosql.VariableName, joinType execution.JoinType, triggers []Trigger) *StreamJoin {
	return &StreamJoin{
		Source:         source,
		Joined:         joined,
		SourceKey:      sourceKey,
		JoinedKey:      joinedKey,
		EventTimeField: eventTimeField,
		JoinType:       joinType,
		Triggers:       triggers,
	}
}

func (node *StreamJoin) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &StreamJoin{
		Source:         node.Source.Transform(ctx, transformers),
		Joined:         node.Joined.Transform(ctx, transformers),
		SourceKey:      node.SourceKey,
		JoinedKey:      node.JoinedKey,
		EventTimeField: node.EventTimeField,
		JoinType:       node.JoinType,
		Triggers:       node.Triggers,
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *StreamJoin) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	materializedSource, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.Joined.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	materializedSourceKey := make([]execution.Expression, len(node.SourceKey))
	materializedJoinedKey := make([]execution.Expression, len(node.JoinedKey))

	for i := range node.SourceKey {
		materializedSourceKey[i], err = node.SourceKey[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize source key expression with index %v", i)
		}

		materializedJoinedKey[i], err = node.JoinedKey[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize joined key expression with index %v", i)
		}
	}

	triggerPrototypes := make([]execution.TriggerPrototype, len(node.Triggers))
	for i := range node.Triggers {
		triggerPrototypes[i], err = node.Triggers[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize trigger with index %v", i)
		}
	}

	var triggerPrototype execution.TriggerPrototype
	if len(triggerPrototypes) == 0 {
		triggerPrototype = execution.NewWatermarkTrigger()
	} else {
		triggerPrototype = execution.NewMultiTrigger(triggerPrototypes...)
	}

	return execution.NewStreamJoin(materializedSource, materializedJoined, materializedSourceKey, materializedJoinedKey, matCtx.Storage, node.EventTimeField, node.JoinType, triggerPrototype), nil
}

func (node *StreamJoin) Metadata() *metadata.NodeMetadata {
	sourceMetadata := node.Source.Metadata()
	joinedMetadata := node.Joined.Metadata()
	cardinality := metadata.CombineCardinalities(sourceMetadata.Cardinality(), joinedMetadata.Cardinality())

	sourceNamespace := sourceMetadata.Namespace()
	sourceNamespace.MergeWith(joinedMetadata.Namespace())

	return metadata.NewNodeMetadata(cardinality, node.EventTimeField, sourceNamespace)
}

func (node *StreamJoin) Visualize() *graph.Node {
	n := graph.NewNode("Stream Join")
	n.AddChild("source", node.Source.Visualize())
	n.AddChild("joined", node.Joined.Visualize())

	for i, expr := range node.SourceKey {
		n.AddChild(fmt.Sprintf("source_key_%d", i), expr.Visualize())
	}

	for i, expr := range node.JoinedKey {
		n.AddChild(fmt.Sprintf("joined_key_%d", i), expr.Visualize())
	}

	for i, trigger := range node.Triggers {
		n.AddChild(fmt.Sprintf("trigger_%d", i), trigger.Visualize())
	}

	n.AddChild("join_type", graph.NewNode(node.JoinType.String()))

	if node.EventTimeField.String() != "" {
		n.AddField("event_time_field", node.EventTimeField.String())
	}
	return n
}

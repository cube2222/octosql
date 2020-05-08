package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type Requalifier struct {
	Qualifier string
	Source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{Qualifier: qualifier, Source: child}
}

func (node *Requalifier) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Requalifier{
		Qualifier: node.Qualifier,
		Source:    node.Source.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Requalifier) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	materialized, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source node")
	}
	return execution.NewRequalifier(node.Qualifier, materialized), nil
}

func (node *Requalifier) Metadata() *metadata.NodeMetadata {
	sourceMetadata := node.Source.Metadata()
	eventTimeField := sourceMetadata.EventTimeField()
	if !eventTimeField.Empty() {
		eventTimeField = octosql.NewVariableName(fmt.Sprintf("%s.%s", node.Qualifier, eventTimeField.Name()))
	}

	namespace := metadata.EmptyNamespace()
	namespace.AddPrefix(node.Qualifier)

	return metadata.NewNodeMetadata(sourceMetadata.Cardinality(), eventTimeField, namespace)
}

func (node *Requalifier) Visualize() *graph.Node {
	n := graph.NewNode("Requalifier")
	n.AddField("qualifier", node.Qualifier)
	n.AddChild("source", node.Source.Visualize())
	return n
}

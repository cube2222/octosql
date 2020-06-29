package physical

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type Distinct struct {
	Source Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{Source: child}
}

func (node *Distinct) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Distinct{
		Source: node.Source.Transform(ctx, transformers),
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}

	return transformed
}

func (node *Distinct) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	garbageCollectionBoundary, err := config.GetInt(matCtx.Config.Execution, "garbageCollectionBoundary", config.WithDefault(600))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get garbageCollectionBoundary configuration")
	}

	garbageCollectionCycle, err := config.GetInt(matCtx.Config.Execution, "garbageCollectionCycle", config.WithDefault(60))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get garbageCollectionCycle configuration")
	}

	childNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node in distinct")
	}

	eventTimeField := node.Source.Metadata().EventTimeField()

	return execution.NewDistinct(matCtx.Storage, childNode, eventTimeField, execution.NewGarbageCollectorInfo(time.Duration(garbageCollectionBoundary), time.Duration(garbageCollectionCycle))), nil
}

func (node *Distinct) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadataFromMetadata(node.Source.Metadata())
}

func (node *Distinct) Visualize() *graph.Node {
	n := graph.NewNode("Distinct")
	n.AddChild("source", node.Source.Visualize())
	return n
}

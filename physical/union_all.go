package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type UnionAll struct {
	Sources []Node
}

func NewUnionAll(sources ...Node) *UnionAll {
	return &UnionAll{Sources: sources}
}

func (node *UnionAll) Transform(ctx context.Context, transformers *Transformers) Node {
	transformedSources := make([]Node, len(node.Sources))
	for i := range node.Sources {
		transformedSources[i] = transformers.NodeT(node.Sources[i])
	}
	var transformed Node = &UnionAll{
		Sources: transformedSources,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *UnionAll) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	materializedSources := make([]execution.Node, len(node.Sources))
	for i := range node.Sources {
		matSource, err := node.Sources[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize union all source with index %d", i)
		}
		materializedSources[i] = matSource
	}

	return execution.NewUnionAll(materializedSources...), nil
}

func (node *UnionAll) Metadata() *metadata.NodeMetadata {
	cardinalities := make([]metadata.Cardinality, len(node.Sources))
	for i := range node.Sources {
		cardinalities[i] = node.Sources[i].Metadata().Cardinality()
	}
	cardinality := metadata.CombineCardinalities(cardinalities...)
	return metadata.NewNodeMetadata(cardinality, node.Sources[0].Metadata().EventTimeField())
}

func (node *UnionAll) Visualize() *graph.Node {
	n := graph.NewNode("Union All")

	for i, source := range node.Sources {
		n.AddChild(fmt.Sprintf("source_%d", i), source.Visualize())
	}
	return n
}

package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type Shuffle struct {
	OutputPartitionCount int
	Strategy             ShuffleStrategy
	Sources              []Node
}

func NewShuffle(outputPartitionCount int, strategy ShuffleStrategy, sourceNodes []Node) []Node {
	shuffle := &Shuffle{
		OutputPartitionCount: outputPartitionCount,
		Strategy:             strategy,
		Sources:              sourceNodes,
	}

	shuffleOutputs := make([]Node, outputPartitionCount)
	for i := 0; i < outputPartitionCount; i++ {
		shuffleOutputs[i] = shuffle
	}

	return shuffleOutputs
}

func (node *Shuffle) Transform(ctx context.Context, transformers *Transformers) Node {
	transformedSources := make([]Node, len(node.Sources))
	for i := range node.Sources {
		transformedSources[i] = node.Sources[i].Transform(ctx, transformers)
	}
	transformedStrategy := node.Strategy.Transform(ctx, transformers)
	var transformed Node = &Shuffle{
		OutputPartitionCount: node.OutputPartitionCount,
		Strategy:             transformedStrategy,
		Sources:              transformedSources,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Shuffle) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	sourceNodes := make([]execution.Node, len(node.Sources))
	for i := range node.Sources {
		matSource, err := node.Sources[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize shuffle input with index %d", i)
		}
		sourceNodes[i] = matSource
	}

	strategyPrototype, err := node.Strategy.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize shuffle strategy")
	}

	return execution.NewShuffle(node.OutputPartitionCount, strategyPrototype, sourceNodes), nil
}

func (node *Shuffle) Metadata() *metadata.NodeMetadata {
	return node.Sources[0].Metadata() // TODO: Go over all inputs and take "maximums"
}

func (node *Shuffle) Visualize() *graph.Node {
	n := graph.NewNode("Shuffle")

	for i, input := range node.Sources {
		n.AddChild(fmt.Sprintf("input_%d", i), input.Visualize())
	}

	n.AddChild("strategy", node.Strategy.Visualize())

	return n
}

type NextShuffleMetadataChange struct {
	ShuffleIDAddSuffix string
	Partition          int
	Source             Node
}

func NewNextShuffleMetadataChange(shuffleIDAddSuffix string, partition int, source Node) *NextShuffleMetadataChange {
	return &NextShuffleMetadataChange{
		ShuffleIDAddSuffix: shuffleIDAddSuffix,
		Partition:          partition,
		Source:             source,
	}
}

func (node *NextShuffleMetadataChange) Transform(ctx context.Context, transformers *Transformers) Node {
	transformedSource := node.Source.Transform(ctx, transformers)
	var transformed Node = &NextShuffleMetadataChange{
		ShuffleIDAddSuffix: node.ShuffleIDAddSuffix,
		Partition:          node.Partition,
		Source:             transformedSource,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *NextShuffleMetadataChange) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	sourceNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't materialize shuffle metadata change input")
	}

	return execution.NewNextShuffleMetadataChange(node.ShuffleIDAddSuffix, node.Partition, sourceNode), nil
}

func (node *NextShuffleMetadataChange) Metadata() *metadata.NodeMetadata {
	return node.Source.Metadata()
}

func (node *NextShuffleMetadataChange) Visualize() *graph.Node {
	n := graph.NewNode("NextShuffleMetadataChange")

	n.AddChild("source", node.Source.Visualize())

	n.AddField("shuffle ID suffix to add", node.ShuffleIDAddSuffix)
	n.AddField("partition", fmt.Sprint(node.Partition))

	return n
}

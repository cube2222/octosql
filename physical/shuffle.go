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
	Key                  []Expression
	Sources              []Node
}

func NewShuffle(outputPartitionCount int, key []Expression, sourceNodes []Node) []Node {
	shuffle := &Shuffle{
		OutputPartitionCount: outputPartitionCount,
		Key:                  key,
		Sources:              sourceNodes,
	}

	shuffleOutputs := make([]Node, outputPartitionCount)
	for i := 0; i < outputPartitionCount; i++ {
		shuffleOutputs[i] = shuffle
	}

	return shuffleOutputs
}

func (s *Shuffle) Transform(ctx context.Context, transformers *Transformers) Node {
	transformedSources := make([]Node, len(s.Sources))
	for i := range s.Sources {
		transformedSources[i] = s.Sources[i].Transform(ctx, transformers)
	}
	transformedKey := make([]Expression, len(s.Key))
	for i := range s.Key {
		transformedKey[i] = s.Key[i].Transform(ctx, transformers)
	}
	var transformed Node = &Shuffle{
		OutputPartitionCount: s.OutputPartitionCount,
		Key:                  transformedKey,
		Sources:              transformedSources,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (s *Shuffle) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	sourceNodes := make([]execution.Node, len(s.Sources))
	for i := range s.Sources {
		matSource, err := s.Sources[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize shuffle input with index %d", i)
		}
		sourceNodes[i] = matSource
	}

	key := make([]execution.Expression, len(s.Key))
	for i := range s.Key {
		matKey, err := s.Key[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize key part with index %d", i)
		}
		key[i] = matKey
	}

	return execution.NewShuffle(s.OutputPartitionCount, key, sourceNodes), nil
}

func (s *Shuffle) Metadata() *metadata.NodeMetadata {
	return s.Sources[0].Metadata()
}

func (s *Shuffle) Visualize() *graph.Node {
	n := graph.NewNode("Shuffle")

	for i, input := range s.Sources {
		n.AddChild(fmt.Sprintf("input_%d", i), input.Visualize())
	}
	return n
}

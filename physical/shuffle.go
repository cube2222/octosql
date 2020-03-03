package physical

import (
	"context"
	"fmt"
	"log"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type ShuffleInput struct {
	Source Node
}

type node struct {
	ShuffleInputs []*ShuffleInput
}

type ShuffleStrategy interface {
	CalculatePartition(record *execution.Record, outputs int) int
}

var DefaultShuffleStrategy ShuffleStrategy = nil

func NewShuffle(outputPartitionCount int, sourceNodes []Node, strategy ShuffleStrategy) []Node {
	if outputPartitionCount != 1 {
		log.Fatal("Shuffle: output partition count other than 1 is not implemented yet")
	}

	shuffleInputs := make([]*ShuffleInput, len(sourceNodes))
	for i := range sourceNodes {
		shuffleInputs[i] = &ShuffleInput{Source: sourceNodes[i]}
	}

	return []Node{&node{ShuffleInputs: shuffleInputs}}
}

func (s *node) Transform(ctx context.Context, transformers *Transformers) Node {
	transformedSources := make([]*ShuffleInput, len(s.ShuffleInputs))
	for i := range s.ShuffleInputs {
		transformedSources[i] = &ShuffleInput{Source: s.ShuffleInputs[i].Source.Transform(ctx, transformers)}
	}
	var transformed Node = &node{
		ShuffleInputs: transformedSources,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (s *node) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	sourceNodes := make([]execution.Node, len(s.ShuffleInputs))
	for i := range s.ShuffleInputs {
		matSource, err := s.ShuffleInputs[i].Source.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize shuffle input with index %d", i)
		}
		sourceNodes[i] = matSource
	}

	return execution.NewUnionAll(sourceNodes...), nil
}

func (s *node) Metadata() *metadata.NodeMetadata {
	return s.ShuffleInputs[0].Source.Metadata()
}

func (s *node) Visualize() *graph.Node {
	n := graph.NewNode("Shuffle")

	for i, input := range s.ShuffleInputs {
		n.AddChild(fmt.Sprintf("input_%d", i), input.Source.Visualize())
	}
	return n
}

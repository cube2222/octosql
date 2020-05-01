package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type StubNode struct {
	NodeMetadata *metadata.NodeMetadata
}

func NewStubNode(metadata *metadata.NodeMetadata) *StubNode {
	return &StubNode{
		NodeMetadata: metadata,
	}
}

func (s *StubNode) Transform(ctx context.Context, transformers *Transformers) Node {
	panic("unreachable")
}

func (s *StubNode) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	panic("unreachable")
}

func (s *StubNode) Metadata() *metadata.NodeMetadata {
	return s.NodeMetadata
}

func (s *StubNode) Visualize() *graph.Node {
	panic("unreachable")
}

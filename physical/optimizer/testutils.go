package optimizer

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
)

// Named placeholder node to use in tests. Easy satisfaction for reflect.DeepEquals.
type PlaceholderNode struct {
	Name string
}

func (node *PlaceholderNode) Transform(ctx context.Context, transformers *physical.Transformers) physical.Node {
	return &PlaceholderNode{
		Name: node.Name,
	}
}

func (*PlaceholderNode) Materialize(ctx context.Context, matCtx *physical.MaterializationContext) (execution.Node, error) {
	panic("tried to materialize a stub node meant only for optimizer tests")
}

func (*PlaceholderNode) Metadata() *metadata.NodeMetadata {
	panic("implement me")
}

func (*PlaceholderNode) Visualize() *graph.Node {
	panic("implement me")
}

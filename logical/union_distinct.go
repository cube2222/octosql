package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
)

type UnionDistinct struct {
	first, second Node
}

func (ua *UnionDistinct) Visualize() *graph.Node {
	n := graph.NewNode("Union Distinct")
	if ua.first != nil {
		n.AddChild("first", ua.first.Visualize())
	}
	if ua.second != nil {
		n.AddChild("second", ua.second.Visualize())
	}
	return n
}

func NewUnionDistinct(first, second Node) *UnionDistinct {
	return &UnionDistinct{first: first, second: second}
}

func (node *UnionDistinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	return NewDistinct(NewUnionAll(node.first, node.second)).Physical(ctx, physicalCreator)
}

package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Distinct struct {
	child Node
}

func (node *Distinct) Visualize() *graph.Node {
	n := graph.NewNode("Distinct")
	if node.child != nil {
		n.AddChild("source", node.child.Visualize())
	}
	return n
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	childNode, variables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get child's physical plan in distinct")
	}

	return physical.NewDistinct(childNode), variables, nil
}

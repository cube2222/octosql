package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type Distinct struct {
	child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	panic("does not work")
	/*sourceNodes, variables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source nodes physical plan in distinct")
	}

	outNodes := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)
	for i := range outNodes {
		outNodes[i] = physical.NewDistinct(outNodes[i])
	}

	return outNodes, variables, nil*/
}

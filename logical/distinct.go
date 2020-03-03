package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Distinct struct {
	child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	childNodes, variables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get child's physical plan in distinct")
	}

	return []physical.Node{physical.NewDistinct(physical.NewUnionAll(childNodes...))}, variables, nil
}

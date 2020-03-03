package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type InnerJoin struct {
	source Node
	joined Node
}

func NewInnerJoin(source Node, joined Node) *InnerJoin {
	return &InnerJoin{source: source, joined: joined}
}

func (node *InnerJoin) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	source, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for map source node")
	}

	joined, joinedVariables, err := node.joined.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for map joined node")
	}

	variables, err := sourceVariables.MergeWith(joinedVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for source and joined nodes")
	}

	return physical.NewInnerJoin(source, joined), variables, nil
}

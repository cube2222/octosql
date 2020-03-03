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
	sourceNodes, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for inner join source node")
	}

	joinedNodes, joinedVariables, err := node.joined.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for inner join joined node")
	}

	variables, err := sourceVariables.MergeWith(joinedVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for source and joined nodes")
	}

	sourceShuffled := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)
	joinedShuffled := physical.NewShuffle(1, joinedNodes, physical.DefaultShuffleStrategy)
	outNodes := make([]physical.Node, len(sourceShuffled))
	for i := range sourceShuffled {
		outNodes[i] = physical.NewInnerJoin(sourceShuffled[i], joinedShuffled[i])
	}

	return outNodes, variables, nil
}

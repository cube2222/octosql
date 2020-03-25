package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type LeftJoin struct {
	source Node
	joined Node
}

func (leftJoin *LeftJoin) Visualize() *graph.Node {
	n := graph.NewNode("LeftJoin")
	if leftJoin.source != nil {
		n.AddChild("source", leftJoin.source.Visualize())
	}
	if leftJoin.joined != nil {
		n.AddChild("joined", leftJoin.joined.Visualize())
	}
	return n
}

func NewLeftJoin(source Node, joined Node) *LeftJoin {
	return &LeftJoin{source: source, joined: joined}
}

func (node *LeftJoin) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
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

	return physical.NewLeftJoin(source, joined), variables, nil
}

package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type Join struct {
	source     Node
	joined     Node
	isLeftJoin bool
}

func NewJoin(source, joined Node, isLeftJoin bool) *Join {
	return &Join{
		source:     source,
		joined:     joined,
		isLeftJoin: isLeftJoin,
	}
}

func (node *Join) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	sourceNodes, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for join source nodes")
	}

	joinedNodes, joinedVariables, err := node.joined.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for left join joined nodes")
	}

	variables, err := sourceVariables.MergeWith(joinedVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for source and joined nodes")
	}

	// Based on the cardinality of sources we decide whether we will create a stream_join or a lookup_join
	// Stream joins support only equity conjunctions (i.e a.x = b.y AND a.v + 17 = b.something * 2)
	sourceCardinality := sourceNodes[0].Metadata().Cardinality()
	joinedCardinality := joinedNodes[0].Metadata().Cardinality()

	isStreamJoin := false

	if sourceCardinality == metadata.Unbounded && joinedCardinality == metadata.Unbounded {
		isStreamJoin = true
	} else if sourceCardinality == metadata.Unbounded && joinedCardinality == metadata.BoundedFitsInLocalStorage {
		isStreamJoin = true
	} else if sourceCardinality == metadata.BoundedFitsInLocalStorage && joinedCardinality == metadata.Unbounded {
		isStreamJoin = true
	}

	if isStreamJoin {
		sourceNamespace := sourceNodes[0].Metadata().Namespace()
		joinedNamespace := joinedNodes[0].Metadata().Namespace()

	}

}

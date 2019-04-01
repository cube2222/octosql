package logical

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Limit struct {
	data   Node
	limit  Node
	offset Node
}

func NewLimit(data, limit, offset Node) *Limit {
	return &Limit{data: data, limit: limit, offset: offset}
}

// I suppose LIMIT ALL support is not going to be implemented soon

func (node *Limit) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	var dataNode, limitNode, offsetNode physical.Node = nil, nil, nil

	if node.data != nil {
		dataNode, dataVariables, err := node.data.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical plan for data node")
		}
		variables, err = variables.MergeWith(dataVariables)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get data node variables")
		}
	}

	//checking if (node.data == nil || (simultaneously node.limit == nil && node.offset == nil)) omitted - parser's job

	if node.limit != nil {
		limitNode, limitVariables, err := node.limit.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical plan for limit node")
		}
		variables, err = variables.MergeWith(limitVariables)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get limit node variables")
		}
	}

	if node.offset != nil {
		offsetNode, offsetVariables, err := node.offset.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical plan for offset node")
		}
		variables, err = variables.MergeWith(offsetVariables)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get offset node variables")
		}
	}

	return physical.NewLimit(dataNode, limitNode, offsetNode), variables, nil
}

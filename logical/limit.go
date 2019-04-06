package logical

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Limit struct {
	data   Node
	limit  Expression
	offset Expression
}

func NewLimit(data Node, limit, offset Expression) *Limit {
	return &Limit{data: data, limit: limit, offset: offset}
}

func (node *Limit) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	if node.data == nil || node.limit == nil || node.offset == nil {
		return nil, nil, errors.New("Limit has a nil field")
	}

	dataNode, variables, err := node.data.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for data node")
	}

	limitExpr, limitVariables, err := node.limit.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for limit expression")
	}
	variables, err = variables.MergeWith(limitVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get limit node variables")
	}

	offsetExpr, offsetVariables, err := node.offset.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for offset expression")
	}
	variables, err = variables.MergeWith(offsetVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get offset node variables")
	}

	return physical.NewLimit(dataNode, limitExpr, offsetExpr), variables, nil
}

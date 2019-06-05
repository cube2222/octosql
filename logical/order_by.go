package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type OrderBy struct {
	Fields []execution.OrderField
	Source Node
}

func NewOrderBy(fields []execution.OrderField, source Node) *OrderBy {
	return &OrderBy{
		Fields: fields,
		Source: source,
	}
}

func (node *OrderBy) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	sourceNode, variables, err := node.Source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan of source node in order by")
	}

	return physical.NewOrderBy(node.Fields, sourceNode), variables, nil
}

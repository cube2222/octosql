package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type OrderDirection string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

type OrderBy struct {
	Expressions []Expression
	Directions  []OrderDirection
	Source      Node
}

func NewOrderBy(expressions []Expression, directions []OrderDirection, source Node) *OrderBy {
	return &OrderBy{
		Expressions: expressions,
		Directions:  directions,
		Source:      source,
	}
}

func (node *OrderBy) Transform(ctx context.Context, transformers *Transformers) Node {
	exprs := make([]Expression, len(node.Expressions))
	for i := range node.Expressions {
		exprs[i] = node.Expressions[i].Transform(ctx, transformers)
	}

	var transformed Node = &OrderBy{
		Expressions: exprs,
		Directions:  node.Directions,
		Source:      node.Source.Transform(ctx, transformers),
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *OrderBy) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	exprs := make([]execution.Expression, len(node.Expressions))
	for i := range node.Expressions {
		var err error
		exprs[i], err = node.Expressions[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}
	}

	directions := make([]execution.OrderDirection, len(node.Expressions))
	for i := range node.Directions {
		directions[i] = execution.OrderDirection(node.Directions[i])
	}

	sourceNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get execution node from order by source")
	}

	return execution.NewOrderBy(exprs, directions, sourceNode), nil
}

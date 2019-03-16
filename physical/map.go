package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Map struct {
	Expressions []NamedExpression
	Source      Node
}

func NewMap(expressions []NamedExpression, child Node) *Map {
	return &Map{Expressions: expressions, Source: child}
}

func (node *Map) Materialize(ctx context.Context) (execution.Node, error) {
	matExprs := make([]execution.NamedExpression, len(node.Expressions))
	for i := range node.Expressions {
		materialized, err := node.Expressions[i].MaterializeNamed(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}
		matExprs[i] = materialized
	}
	materialized, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source node")
	}

	return execution.NewMap(matExprs, materialized), nil
}

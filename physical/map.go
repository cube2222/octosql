package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

type Map struct {
	Expressions []NamedExpression
	Source      Node
}

func NewMap(expressions []NamedExpression, child Node) *Map {
	return &Map{Expressions: expressions, Source: child}
}

func (node *Map) Materialize(ctx context.Context) execution.Node {
	matExprs := make([]execution.NamedExpression, len(node.Expressions))
	for i := range node.Expressions {
		matExprs[i] = node.Expressions[i].MaterializeNamed(ctx)
	}

	return execution.NewMap(matExprs, node.Source.Materialize(ctx))
}

package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

type Map struct {
	expressions []NamedExpression
	source      Node
}

func NewMap(expressions []NamedExpression, child Node) *Map {
	return &Map{expressions: expressions, source: child}
}

func (node *Map) Materialize(ctx context.Context) execution.Node {
	matExprs := make([]execution.NamedExpression, len(node.expressions))
	for i := range node.expressions {
		matExprs[i] = node.expressions[i].MaterializeNamed(ctx)
	}

	return execution.NewMap(matExprs, node.source.Materialize(ctx))
}

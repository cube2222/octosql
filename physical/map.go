package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type Map struct {
	Expressions []NamedExpression
	Source      Node
	Keep        bool
}

func NewMap(expressions []NamedExpression, child Node, keep bool) *Map {
	return &Map{Expressions: expressions, Source: child, Keep: keep}
}

func (node *Map) Transform(ctx context.Context, transformers *Transformers) Node {
	exprs := make([]NamedExpression, len(node.Expressions))
	for i := range node.Expressions {
		exprs[i] = node.Expressions[i].TransformNamed(ctx, transformers)
	}
	var transformed Node = &Map{
		Expressions: exprs,
		Source:      node.Source.Transform(ctx, transformers),
		Keep:        node.Keep,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *Map) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	matExprs := make([]execution.NamedExpression, len(node.Expressions))
	for i := range node.Expressions {
		materialized, err := node.Expressions[i].MaterializeNamed(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}
		matExprs[i] = materialized
	}
	materialized, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source node")
	}

	return execution.NewMap(matExprs, materialized, node.Keep), nil
}

func (node *Map) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(node.Source.Metadata().Cardinality())
}

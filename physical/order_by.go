package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
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

func (node *OrderBy) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &OrderBy{
		Fields: node.Fields,
		Source: node.Source.Transform(ctx, transformers),
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *OrderBy) Materialize(ctx context.Context) (execution.Node, error) {
	sourceNode, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get execution node from order by source")
	}

	return execution.NewOrderBy(node.Fields, sourceNode), nil
}

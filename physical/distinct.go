package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Distinct struct {
	Child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{Child: child}
}

func (node *Distinct) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Distinct{
		Child: node.Child.Transform(ctx, transformers),
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}

	return transformed
}

func (node *Distinct) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	childNode, err := node.Child.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize child node in distinct")
	}

	return execution.NewDistinct(childNode), nil
}

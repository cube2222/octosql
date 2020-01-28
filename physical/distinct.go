package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type Distinct struct {
	Source Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{Source: child}
}

func (node *Distinct) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &Distinct{
		Source: node.Source.Transform(ctx, transformers),
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}

	return transformed
}

func (node *Distinct) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	childNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node in distinct")
	}

	return execution.NewDistinct(childNode), nil
}

func (node *Distinct) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(node.Source.Metadata().Cardinality(), node.Source.Metadata().EventTimeField())
}

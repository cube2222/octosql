package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type Distinct struct {
	source Node
}

func NewDistinct(source Node) *Distinct {
	return &Distinct{source: source}
}

func (node *Distinct) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	source := node.source.Typecheck(ctx, env, logicalEnv)

	return physical.Node{
		Schema:   source.Schema,
		NodeType: physical.NodeTypeDistinct,
		Distinct: &physical.Distinct{
			Source: source,
		},
	}
}

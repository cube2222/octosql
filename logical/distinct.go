package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type Distinct struct {
	child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Node {
	panic("implement me")
}

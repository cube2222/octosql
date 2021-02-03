package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type UnionDistinct struct {
	first, second Node
}

func NewUnionDistinct(first, second Node) *UnionDistinct {
	return &UnionDistinct{first: first, second: second}
}

func (node *UnionDistinct) Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error) {
	panic("implement me")
	// return NewDistinct(NewUnionAll(node.first, node.second)).Physical(ctx, physicalCreator)
}

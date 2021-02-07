package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	panic("implement me")
}

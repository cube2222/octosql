package logical

import (
	"github.com/cube2222/octosql/physical"

	"context"
)

type FunctionExpression struct {
	name      string
	arguments []Expression
}

func NewFunctionExpression(name string, args []Expression) *FunctionExpression {
	return &FunctionExpression{
		name:      name,
		arguments: args,
	}
}

func (fe *FunctionExpression) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	panic("implement me")
}

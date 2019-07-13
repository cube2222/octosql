package physical

import (
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/functions"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

/* This assumes that the allowed functions map single value
to single value and can throw errors (i.e when you try to
lowercase an int). This may be expanded in the future
to make the functions more versatile.
*/

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

func (fe *FunctionExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	transformed := make([]Expression, 0)
	for i := range fe.arguments {
		transformedArg := fe.arguments[i].Transform(ctx, transformers)
		transformed = append(transformed, transformedArg)
	}

	var expr Expression = NewFunctionExpression(fe.name, transformed)

	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (fe *FunctionExpression) Materialize(ctx context.Context) (execution.Expression, error) {
	function, ok := functionTable[fe.name]
	if !ok {
		return nil, errors.Errorf("No function %v found", fe.name)
	}

	materialized := make([]execution.Expression, 0)
	for i := range fe.arguments {
		materializedArg, err := fe.arguments[i].Materialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize argument")
		}

		materialized = append(materialized, materializedArg)
	}

	return execution.NewFunctionExpression(function, materialized), nil
}

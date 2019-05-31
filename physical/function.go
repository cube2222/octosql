package physical

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

/* This assumes that the allowed functions map single value
to single value and can throw errors (i.e when you try to
lowercase an int). This may be expanded in the future
to make the functions more versatile.
*/

/* The only legal functions are the ones that appear in this
table. Otherwise the function will be considered undefined
and will throw an error on physical -> execution conversion.
*/
var functionTable = map[string]execution.FunctionType{
	"int":   execution.FuncInt,
	"lower": execution.FuncLower,
	"upper": execution.FuncUpper,
	"neg":   execution.FuncNegative,
	"abs":   execution.FuncAbs,
	"cap":   execution.FuncCapitalize,
	"sqrt":  execution.FuncSqrt,
}

type FunctionExpression struct {
	Name  string
	Child Expression
}

func NewFunctionExpression(name string, child Expression) *FunctionExpression {
	return &FunctionExpression{
		Name:  name,
		Child: child,
	}
}

func (fe *FunctionExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	var expr Expression = &FunctionExpression{
		Name:  fe.Name,
		Child: fe.Child.Transform(ctx, transformers),
	}
	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (fe *FunctionExpression) Materialize(ctx context.Context) (execution.Expression, error) {
	materialized, err := fe.Child.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize child")
	}

	function, ok := functionTable[fe.Name]
	if !ok {
		return nil, errors.Errorf("No function %v found", fe.Name)
	}

	return execution.NewFunctionExpression(function, materialized), nil
}

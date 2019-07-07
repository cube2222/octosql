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

/* The only legal functions are the ones that appear in this
table. Otherwise the function will be considered undefined
and will throw an error on physical -> execution conversion.
IMPORTANT: As of now the lookup is case sensitive, so the functions must
be stored in lowercase, and the user must input them as lowercase as well.
*/
var functionTable = map[string]execution.Function{
	"int":          functions.FuncInt,
	"float":        functions.FuncFloat,
	"lowercase":    functions.FuncLower,
	"uppercase":    functions.FuncUpper,
	"negate":       functions.FuncNegate,
	"abs":          functions.FuncAbs,
	"capitalize":   functions.FuncCapitalize,
	"sqrt":         functions.FuncSqrt,
	"greatest":     functions.FuncGreatest,
	"least":        functions.FuncLeast,
	"randint":      functions.FuncRandInt,
	"randfloat":    functions.FuncRandFloat,
	"ceiling":      functions.FuncCeil,
	"floor":        functions.FuncFloor,
	"log":          functions.FuncLog,
	"ln":           functions.FuncLn,
	"power":        functions.FuncPower,
	"reverse":      functions.FuncReverse,
	"sub":          functions.FuncSubstring, //TODO: fix parsing so that you can name this function substring
	"matchregular": functions.FuncRegexp,
}

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

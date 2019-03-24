package optimizer

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

func GetVariables(ctx context.Context, expr physical.Expression) []octosql.VariableName {
	variables := make(map[octosql.VariableName]struct{})

	transformers := &physical.Transformers{
		NodeT: nil,
		ExprT: nil,
		NamedExprT: func(expr physical.NamedExpression) physical.NamedExpression {
			if expr, ok := expr.(*physical.Variable); ok {
				variables[expr.Name] = struct{}{}
			}
			return expr
		},
		FormulaT: nil,
	}

	expr.Transform(ctx, transformers)

	out := make([]octosql.VariableName, 0, len(variables))
	for variable := range variables {
		out = append(out, variable)
	}
	return out
}

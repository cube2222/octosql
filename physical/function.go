package physical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/functions"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"

	"github.com/pkg/errors"
)

/* This assumes that the allowed functions map single value
to single value and can throw errors (i.e when you try to
lowercase an int). This may be expanded in the future
to make the functions more versatile.
*/

type FunctionExpression struct {
	Name      string
	Arguments []Expression
}

func NewFunctionExpression(name string, args []Expression) *FunctionExpression {
	return &FunctionExpression{
		Name:      name,
		Arguments: args,
	}
}

func (fe *FunctionExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	transformed := make([]Expression, 0)
	for i := range fe.Arguments {
		transformedArg := fe.Arguments[i].Transform(ctx, transformers)
		transformed = append(transformed, transformedArg)
	}

	var expr Expression = NewFunctionExpression(fe.Name, transformed)

	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (fe *FunctionExpression) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	function, ok := functions.FunctionTable[fe.Name]
	if !ok {
		return nil, errors.Errorf("No function %v found", fe.Name)
	}

	materialized := make([]execution.Expression, 0)
	for i := range fe.Arguments {
		materializedArg, err := fe.Arguments[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize argument")
		}

		materialized = append(materialized, materializedArg)
	}

	return execution.NewFunctionExpression(function, materialized), nil
}

func (fe *FunctionExpression) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	for _, expr := range fe.Arguments {
		if !expr.DoesMatchNamespace(namespace) {
			return false
		}
	}

	return true
}

func (fe *FunctionExpression) Visualize() *graph.Node {
	n := graph.NewNode(fe.Name)
	for i, arg := range fe.Arguments {
		n.AddChild(fmt.Sprintf("arg_%d", i), arg.Visualize())
	}
	return n
}

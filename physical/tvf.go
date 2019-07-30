package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/tvf"
	"github.com/pkg/errors"
)

type TableValuedFunction struct {
	Name      string
	Arguments map[octosql.VariableName]Expression
}

func NewTableValuedFunction(name string, args map[octosql.VariableName]Expression) *TableValuedFunction {
	return &TableValuedFunction{
		Name:      name,
		Arguments: args,
	}
}

func (node *TableValuedFunction) Transform(ctx context.Context, transformers *Transformers) Node {
	Arguments := make(map[octosql.VariableName]Expression, len(node.Arguments))
	for i := range node.Arguments {
		Arguments[i] = node.Arguments[i].Transform(ctx, transformers)
	}
	var transformed Node = &TableValuedFunction{
		Name:      node.Name,
		Arguments: Arguments,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *TableValuedFunction) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	// In this switch you'd for example type assert an expression into a NodeExpression,
	// and take out the underlying Node to be a direct child of the TVF.
	switch node.Name {
	case "range":
		startExpr, ok := node.Arguments["range_start"]
		if !ok {
			return nil, errors.Errorf("required parameter start not provided")
		}
		endExpr, ok := node.Arguments["range_end"]
		if !ok {
			return nil, errors.Errorf("required parameter end not provided")
		}

		startMat, err := startExpr.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Errorf("couldn't materialize start of range expression")
		}
		endMat, err := endExpr.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Errorf("couldn't materialize end of range expression")
		}

		return tvf.NewRange(startMat, endMat), nil

	}

	return nil, errors.Errorf("invalid table valued function: %v", node.Name)
}

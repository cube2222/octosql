package physical

import (
	"context"
	"reflect"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/tvf"
	"github.com/cube2222/octosql/physical/metadata"
)

type TableValuedFunctionArgumentValue interface {
	iTableValuedFunctionArgumentValue()
	Transform(ctx context.Context, transformers *Transformers) TableValuedFunctionArgumentValue
}

func (*TableValuedFunctionArgumentValueExpression) iTableValuedFunctionArgumentValue() {}
func (*TableValuedFunctionArgumentValueTable) iTableValuedFunctionArgumentValue()      {}
func (*TableValuedFunctionArgumentValueDescriptor) iTableValuedFunctionArgumentValue() {}

type TableValuedFunctionArgumentValueExpression struct {
	Expression Expression
}

func NewTableValuedFunctionArgumentValueExpression(expression Expression) *TableValuedFunctionArgumentValueExpression {
	return &TableValuedFunctionArgumentValueExpression{Expression: expression}
}

func (arg *TableValuedFunctionArgumentValueExpression) Transform(ctx context.Context, transformers *Transformers) TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValueExpression{Expression: arg.Expression.Transform(ctx, transformers)}
}

type TableValuedFunctionArgumentValueTable struct {
	Source Node
}

func NewTableValuedFunctionArgumentValueTable(source Node) *TableValuedFunctionArgumentValueTable {
	return &TableValuedFunctionArgumentValueTable{Source: source}
}

func (arg *TableValuedFunctionArgumentValueTable) Transform(ctx context.Context, transformers *Transformers) TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValueTable{Source: arg.Source.Transform(ctx, transformers)}
}

type TableValuedFunctionArgumentValueDescriptor struct {
	Descriptor string
}

func NewTableValuedFunctionArgumentValueDescriptor(descriptor string) *TableValuedFunctionArgumentValueDescriptor {
	return &TableValuedFunctionArgumentValueDescriptor{Descriptor: descriptor}
}

func (arg *TableValuedFunctionArgumentValueDescriptor) Transform(ctx context.Context, transformers *Transformers) TableValuedFunctionArgumentValue {
	var transformed TableValuedFunctionArgumentValue = &TableValuedFunctionArgumentValueDescriptor{
		Descriptor: arg.Descriptor,
	}
	if transformers.TableValuedFunctionArgumentValueT != nil {
		transformed = transformers.TableValuedFunctionArgumentValueT(transformed)
	}
	return transformed
}

type TableValuedFunction struct {
	Name      string
	Arguments map[octosql.VariableName]TableValuedFunctionArgumentValue
}

func NewTableValuedFunction(name string, args map[octosql.VariableName]TableValuedFunctionArgumentValue) *TableValuedFunction {
	return &TableValuedFunction{
		Name:      name,
		Arguments: args,
	}
}

func (node *TableValuedFunction) Transform(ctx context.Context, transformers *Transformers) Node {
	Arguments := make(map[octosql.VariableName]TableValuedFunctionArgumentValue, len(node.Arguments))
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

func (node *TableValuedFunction) getArgumentExpression(name octosql.VariableName) (Expression, error) {
	arg, ok := node.Arguments[name]
	if !ok {
		return nil, errors.Errorf("argument %v not provided", name)
	}
	argExpression, ok := arg.(*TableValuedFunctionArgumentValueExpression)
	if !ok {
		return nil, errors.Errorf("argument %v should be expression, is %v", name, reflect.TypeOf(arg))
	}

	return argExpression.Expression, nil
}

func (node *TableValuedFunction) getArgumentTable(name octosql.VariableName) (Node, error) {
	arg, ok := node.Arguments[name]
	if !ok {
		return nil, errors.Errorf("argument %v not provided", name)
	}
	argExpression, ok := arg.(*TableValuedFunctionArgumentValueTable)
	if !ok {
		return nil, errors.Errorf("argument %v should be table, is %v", name, reflect.TypeOf(arg))
	}

	return argExpression.Source, nil
}

func (node *TableValuedFunction) getArgumentDescriptor(name octosql.VariableName) (string, error) {
	arg, ok := node.Arguments[name]
	if !ok {
		return "", errors.Errorf("argument %v not provided", name)
	}
	argExpression, ok := arg.(*TableValuedFunctionArgumentValueDescriptor)
	if !ok {
		return "", errors.Errorf("argument %v should be field descriptor, is %v", name, reflect.TypeOf(arg))
	}

	return argExpression.Descriptor, nil
}

func (node *TableValuedFunction) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	// In this switch you'd for example type assert an expression into a NodeExpression,
	// and take out the underlying Node to be a direct child of the TVF.
	switch node.Name {
	case "range":
		startExpr, err := node.getArgumentExpression("range_start")
		if err != nil {
			return nil, err
		}
		endExpr, err := node.getArgumentExpression("range_end")
		if err != nil {
			return nil, err
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

func (node *TableValuedFunction) Metadata() *metadata.NodeMetadata {
	switch node.Name {
	case "range":
		return metadata.NewNodeMetadata(metadata.BoundedFitsInLocalStorage)
	default:
		return metadata.NewNodeMetadata(metadata.Unbounded)
	}
}

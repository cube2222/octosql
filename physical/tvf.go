package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/tvf"
	"github.com/cube2222/octosql/physical/metadata"
)

type TableValuedFunctionArgumentValueType string

const (
	TableValuedFunctionArgumentValueExpression TableValuedFunctionArgumentValueType = "expression"
	TableValuedFunctionArgumentValueTable      TableValuedFunctionArgumentValueType = "table"
	TableValuedFunctionArgumentValueDescriptor TableValuedFunctionArgumentValueType = "descriptor"
)

type TableValuedFunctionArgumentValue struct {
	ArgumentType TableValuedFunctionArgumentValueType
	Expression   Expression
	Source       Node
	descriptor   string
}

func NewTableValuedFunctionArgumentValueExpression(expression Expression) *TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValue{ArgumentType: TableValuedFunctionArgumentValueExpression, Expression: expression}
}

func NewTableValuedFunctionArgumentValueTable(source Node) *TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValue{ArgumentType: TableValuedFunctionArgumentValueTable, Source: source}
}

func NewTableValuedFunctionArgumentValueDescriptor(descriptor string) *TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValue{ArgumentType: TableValuedFunctionArgumentValueDescriptor, descriptor: descriptor}
}

func (arg *TableValuedFunctionArgumentValue) Transform(ctx context.Context, transformers *Transformers) *TableValuedFunctionArgumentValue {
	transformed := &TableValuedFunctionArgumentValue{
		ArgumentType: arg.ArgumentType,
	}

	switch arg.ArgumentType {
	case TableValuedFunctionArgumentValueExpression:
		transformed.Expression = arg.Expression.Transform(ctx, transformers)

	case TableValuedFunctionArgumentValueTable:
		transformed.Source = arg.Source.Transform(ctx, transformers)

	case TableValuedFunctionArgumentValueDescriptor:
		transformed.descriptor = arg.descriptor

	default:
		panic(errors.Errorf("invalid table valued function argument type: %v", arg.ArgumentType))
	}

	if transformers.TableValuedFunctionArgumentValueT != nil {
		transformed = transformers.TableValuedFunctionArgumentValueT(transformed)
	}
	return transformed
}

type TableValuedFunction struct {
	Name      string
	Arguments map[octosql.VariableName]*TableValuedFunctionArgumentValue
}

func NewTableValuedFunction(name string, args map[octosql.VariableName]*TableValuedFunctionArgumentValue) *TableValuedFunction {
	return &TableValuedFunction{
		Name:      name,
		Arguments: args,
	}
}

func (node *TableValuedFunction) Transform(ctx context.Context, transformers *Transformers) Node {
	Arguments := make(map[octosql.VariableName]*TableValuedFunctionArgumentValue, len(node.Arguments))
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
	if arg.ArgumentType != TableValuedFunctionArgumentValueExpression {
		return nil, errors.Errorf("argument %v should be expression, is %v", name, arg.ArgumentType)
	}

	return arg.Expression, nil
}

func (node *TableValuedFunction) getArgumentTable(name octosql.VariableName) (Node, error) {
	arg, ok := node.Arguments[name]
	if !ok {
		return nil, errors.Errorf("argument %v not provided", name)
	}
	if arg.ArgumentType != TableValuedFunctionArgumentValueTable {
		return nil, errors.Errorf("argument %v should be table, is %v", name, arg.ArgumentType)
	}

	return arg.Source, nil
}

func (node *TableValuedFunction) getArgumentDescriptor(name octosql.VariableName) (string, error) {
	arg, ok := node.Arguments[name]
	if !ok {
		return "", errors.Errorf("argument %v not provided", name)
	}
	if arg.ArgumentType != TableValuedFunctionArgumentValueDescriptor {
		return "", errors.Errorf("argument %v should be field descriptor, is %v", name, arg.ArgumentType)
	}

	return arg.descriptor, nil
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

	panic(fmt.Sprintf("%+v", node.Arguments["data"]))

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

package logical

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type TableValuedFunctionArgumentValueType string

const (
	TableValuedFunctionArgumentValueExpression TableValuedFunctionArgumentValueType = "expression"
	TableValuedFunctionArgumentValueTable      TableValuedFunctionArgumentValueType = "table"
	TableValuedFunctionArgumentValueDescriptor TableValuedFunctionArgumentValueType = "descriptor"
)

type TableValuedFunctionArgumentValue struct {
	argumentType TableValuedFunctionArgumentValueType
	expression   Expression
	source       Node
	descriptor   string
}

func NewTableValuedFunctionArgumentValueExpression(expression Expression) *TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValue{argumentType: TableValuedFunctionArgumentValueExpression, expression: expression}
}

func NewTableValuedFunctionArgumentValueTable(source Node) *TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValue{argumentType: TableValuedFunctionArgumentValueTable, source: source}
}

func NewTableValuedFunctionArgumentValueDescriptor(descriptor string) *TableValuedFunctionArgumentValue {
	return &TableValuedFunctionArgumentValue{argumentType: TableValuedFunctionArgumentValueDescriptor, descriptor: descriptor}
}

func (arg *TableValuedFunctionArgumentValue) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (*physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	switch arg.argumentType {
	case TableValuedFunctionArgumentValueExpression:
		physExpression, variables, err := arg.expression.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical expression")
		}
		return physical.NewTableValuedFunctionArgumentValueExpression(physExpression), variables, nil

	case TableValuedFunctionArgumentValueTable:
		physNode, variables, err := arg.source.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical expression")
		}
		return physical.NewTableValuedFunctionArgumentValueTable(physNode), variables, nil

	case TableValuedFunctionArgumentValueDescriptor:
		return physical.NewTableValuedFunctionArgumentValueDescriptor(arg.descriptor), octosql.NoVariables(), nil

	default:
		return nil, nil, errors.Errorf("invalid table valued function argument type: %v", arg.argumentType)
	}
}

type TableValuedFunction struct {
	name      string
	arguments map[octosql.VariableName]*TableValuedFunctionArgumentValue
}

func NewTableValuedFunction(name string, arguments map[octosql.VariableName]*TableValuedFunctionArgumentValue) *TableValuedFunction {
	return &TableValuedFunction{name: name, arguments: arguments}
}

func (node *TableValuedFunction) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	physArguments := make(map[octosql.VariableName]*physical.TableValuedFunctionArgumentValue)
	for k, v := range node.arguments {
		physArg, argVariables, err := v.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't get physical plan for table valued function argument \"%s\"", k,
			)
		}

		variables, err = variables.MergeWith(argVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't merge variables with those of table valued function argument \"%s\"", k,
			)
		}

		physArguments[k] = physArg
	}

	return physical.NewTableValuedFunction(
		node.name,
		physArguments,
	), variables, nil
}

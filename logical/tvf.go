package logical

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type TableValuedFunctionArgumentValue interface {
	iTableValuedFunctionArgumentValue()
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.TableValuedFunctionArgumentValue, octosql.Variables, error)
}

func (*TableValuedFunctionArgumentValueExpression) iTableValuedFunctionArgumentValue() {}
func (*TableValuedFunctionArgumentValueTable) iTableValuedFunctionArgumentValue()      {}
func (*TableValuedFunctionArgumentValueDescriptor) iTableValuedFunctionArgumentValue() {}

type TableValuedFunctionArgumentValueExpression struct {
	expression Expression
}

func NewTableValuedFunctionArgumentValueExpression(expression Expression) *TableValuedFunctionArgumentValueExpression {
	return &TableValuedFunctionArgumentValueExpression{expression: expression}
}

func (arg *TableValuedFunctionArgumentValueExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	physExpression, variables, err := arg.expression.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical expression")
	}

	return physical.NewTableValuedFunctionArgumentValueExpression(physExpression), variables, nil
}

type TableValuedFunctionArgumentValueTable struct {
	source Node
}

func NewTableValuedFunctionArgumentValueTable(source Node) *TableValuedFunctionArgumentValueTable {
	return &TableValuedFunctionArgumentValueTable{source: source}
}

func (arg *TableValuedFunctionArgumentValueTable) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	physExpression, variables, err := arg.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical node")
	}

	return physical.NewTableValuedFunctionArgumentValueTable(physExpression), variables, nil
}

type TableValuedFunctionArgumentValueDescriptor struct {
	descriptor octosql.VariableName
}

func NewTableValuedFunctionArgumentValueDescriptor(descriptor octosql.VariableName) *TableValuedFunctionArgumentValueDescriptor {
	return &TableValuedFunctionArgumentValueDescriptor{descriptor: descriptor}
}

func (arg *TableValuedFunctionArgumentValueDescriptor) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	return physical.NewTableValuedFunctionArgumentValueDescriptor(arg.descriptor), octosql.NoVariables(), nil
}

type TableValuedFunction struct {
	name      string
	arguments map[octosql.VariableName]TableValuedFunctionArgumentValue
}

func NewTableValuedFunction(name string, arguments map[octosql.VariableName]TableValuedFunctionArgumentValue) *TableValuedFunction {
	return &TableValuedFunction{name: name, arguments: arguments}
}

func (node *TableValuedFunction) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	physArguments := make(map[octosql.VariableName]physical.TableValuedFunctionArgumentValue)
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

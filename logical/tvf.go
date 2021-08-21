package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type TableValuedFunctionDescription struct {
	TypecheckArguments func(context.Context, physical.Environment, Environment, map[string]TableValuedFunctionArgumentValue) map[string]TableValuedFunctionTypecheckedArgument
	Descriptors        []TableValuedFunctionDescriptor
}

type TableValuedFunctionTypecheckedArgument struct {
	Mapping  map[string]string
	Argument physical.TableValuedFunctionArgument
}

type TableValuedFunctionDescriptor struct {
	Arguments map[string]TableValuedFunctionArgumentMatcher
	// Here we can check the inputs.
	OutputSchema func(context.Context, physical.Environment, Environment, map[string]TableValuedFunctionTypecheckedArgument) (physical.Schema, map[string]string, error)
	Materialize  func(context.Context, physical.Environment, map[string]physical.TableValuedFunctionArgument) (execution.Node, error)
}

type TableValuedFunctionArgumentMatcher struct {
	Required                               bool
	TableValuedFunctionArgumentMatcherType physical.TableValuedFunctionArgumentType
	// Only one of the below may be non-null.
	Expression *TableValuedFunctionArgumentMatcherExpression
	Table      *TableValuedFunctionArgumentMatcherTable
	Descriptor *TableValuedFunctionArgumentMatcherDescriptor
}

type TableValuedFunctionArgumentMatcherExpression struct {
	Type octosql.Type
}

type TableValuedFunctionArgumentMatcherTable struct {
}

type TableValuedFunctionArgumentMatcherDescriptor struct {
}

type TableValuedFunctionArgumentValue interface {
	iTableValuedFunctionArgumentValue()
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

func (arg *TableValuedFunctionArgumentValueExpression) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.TableValuedFunctionArgument {
	return physical.TableValuedFunctionArgument{
		TableValuedFunctionArgumentType: physical.TableValuedFunctionArgumentTypeExpression,
		Expression: &physical.TableValuedFunctionArgumentExpression{
			Expression: arg.expression.Typecheck(ctx, env, logicalEnv),
		},
	}
}

type TableValuedFunctionArgumentValueTable struct {
	source Node
}

func NewTableValuedFunctionArgumentValueTable(source Node) *TableValuedFunctionArgumentValueTable {
	return &TableValuedFunctionArgumentValueTable{source: source}
}

func (arg *TableValuedFunctionArgumentValueTable) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.TableValuedFunctionArgument, map[string]string) {
	node, mapping := arg.source.Typecheck(ctx, env, logicalEnv)
	return physical.TableValuedFunctionArgument{
		TableValuedFunctionArgumentType: physical.TableValuedFunctionArgumentTypeTable,
		Table: &physical.TableValuedFunctionArgumentTable{
			Table: node,
		},
	}, mapping
}

type TableValuedFunctionArgumentValueDescriptor struct {
	descriptor string
}

func NewTableValuedFunctionArgumentValueDescriptor(descriptor string) *TableValuedFunctionArgumentValueDescriptor {
	return &TableValuedFunctionArgumentValueDescriptor{descriptor: descriptor}
}

func (arg *TableValuedFunctionArgumentValueDescriptor) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.TableValuedFunctionArgument {
	unique, ok := logicalEnv.UniqueVariableNames.GetUniqueName(arg.descriptor)
	if !ok {
		panic(fmt.Errorf("no %s field in source stream", arg.descriptor))
	}
	return physical.TableValuedFunctionArgument{
		TableValuedFunctionArgumentType: physical.TableValuedFunctionArgumentTypeDescriptor,
		Descriptor: &physical.TableValuedFunctionArgumentDescriptor{
			Descriptor: unique,
		},
	}
}

type TableValuedFunction struct {
	name      string
	arguments map[string]TableValuedFunctionArgumentValue
}

func NewTableValuedFunction(name string, arguments map[string]TableValuedFunctionArgumentValue) *TableValuedFunction {
	return &TableValuedFunction{name: name, arguments: arguments}
}

func (node *TableValuedFunction) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	description := logicalEnv.TableValuedFunctions[node.name]

	// TODO: First check if required arguments are present. Without checking their type.
	// Just check if the "kind" is right. So table, descriptor, expression.

	arguments := description.TypecheckArguments(ctx, env, logicalEnv, node.arguments)

	physicalArguments := make(map[string]physical.TableValuedFunctionArgument)
	for k, v := range arguments {
		physicalArguments[k] = v.Argument
	}

descriptorLoop:
	for _, descriptor := range description.Descriptors {
		for name, arg := range descriptor.Arguments {
			if arg.Required {
				if _, ok := arguments[name]; !ok {
					continue descriptorLoop
				}
			}
		}
		for name, arg := range arguments {
			matcher, ok := descriptor.Arguments[name]
			if !ok {
				continue descriptorLoop
			}
			if arg.Argument.TableValuedFunctionArgumentType != matcher.TableValuedFunctionArgumentMatcherType {
				continue descriptorLoop
			}
			switch arg.Argument.TableValuedFunctionArgumentType {
			case physical.TableValuedFunctionArgumentTypeExpression:
				if rel := arg.Argument.Expression.Expression.Type.Is(matcher.Expression.Type); rel < octosql.TypeRelationIs {
					continue descriptorLoop
				}
			case physical.TableValuedFunctionArgumentTypeTable:
			case physical.TableValuedFunctionArgumentTypeDescriptor:
			}
		}
		outSchema, outputMapping, err := descriptor.OutputSchema(ctx, env, logicalEnv, arguments)
		if err != nil {
			panic(err)
		}
		return physical.Node{
			Schema:   outSchema,
			NodeType: physical.NodeTypeTableValuedFunction,
			TableValuedFunction: &physical.TableValuedFunction{
				Name:      node.name,
				Arguments: physicalArguments,
				FunctionDescriptor: physical.TableValuedFunctionDescriptor{
					Materialize: descriptor.Materialize,
				},
			},
		}, outputMapping
	}
descriptorLoop2:
	for _, descriptor := range description.Descriptors {
		for name, arg := range descriptor.Arguments {
			if arg.Required {
				if _, ok := arguments[name]; !ok {
					continue descriptorLoop2
				}
			}
		}
		for name, arg := range arguments {
			matcher, ok := descriptor.Arguments[name]
			if !ok {
				continue descriptorLoop2
			}
			if arg.Argument.TableValuedFunctionArgumentType != matcher.TableValuedFunctionArgumentMatcherType {
				continue descriptorLoop2
			}
			switch arg.Argument.TableValuedFunctionArgumentType {
			case physical.TableValuedFunctionArgumentTypeExpression:
				if rel := arg.Argument.Expression.Expression.Type.Is(matcher.Expression.Type); rel < octosql.TypeRelationMaybe {
					continue descriptorLoop2
				}
			case physical.TableValuedFunctionArgumentTypeTable:
			case physical.TableValuedFunctionArgumentTypeDescriptor:
			}
		}
		outSchema, outputMapping, err := descriptor.OutputSchema(ctx, env, logicalEnv, arguments)
		if err != nil {
			panic(err)
		}
		return physical.Node{
			Schema:   outSchema,
			NodeType: physical.NodeTypeTableValuedFunction,
			TableValuedFunction: &physical.TableValuedFunction{
				Name:      node.name,
				Arguments: physicalArguments,
				FunctionDescriptor: physical.TableValuedFunctionDescriptor{
					Materialize: descriptor.Materialize,
				},
			},
		}, outputMapping
	}
	var argNames []string
	for k, v := range physicalArguments {
		argNames = append(argNames, fmt.Sprintf("%s=>%s", k, v.String()))
	}
	// TODO: Print available overloads.
	panic(fmt.Sprintf("unknown table valued function: %s(%s)", node.name, strings.Join(argNames, ", ")))
}

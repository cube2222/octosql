package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

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

func (fe *FunctionExpression) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	arguments := make([]physical.Expression, len(fe.Arguments))
	for i := range fe.Arguments {
		arguments[i] = fe.Arguments[i].Typecheck(ctx, env, logicalEnv)
	}
	argumentTypes := make([]octosql.Type, len(arguments))
	nonNullableArgumentTypes := make([]octosql.Type, len(arguments))
	for i := range arguments {
		argumentTypes[i] = arguments[i].Type
		nonNullableArgumentTypes[i] = octosql.NonNullable(arguments[i].Type)
	}

	var out physical.Expression
	var found bool

	details := env.Functions[fe.Name]
descriptorLoop:
	for _, descriptor := range details.Descriptors {
		argTypes := argumentTypes
		if descriptor.Strict {
			argTypes = nonNullableArgumentTypes
		}
		if descriptor.TypeFn != nil {
			if outputType, ok := descriptor.TypeFn(argTypes); ok {
				found = true
				out = physical.Expression{
					Type:           outputType,
					ExpressionType: physical.ExpressionTypeFunctionCall,
					FunctionCall: &physical.FunctionCall{
						Name:               fe.Name,
						Arguments:          arguments,
						FunctionDescriptor: descriptor,
					},
				}
			}
		} else {
			if len(argTypes) != len(descriptor.ArgumentTypes) {
				continue
			}
			for i := range argTypes {
				if argTypes[i].Is(descriptor.ArgumentTypes[i]) < octosql.TypeRelationIs {
					continue descriptorLoop
				}
			}
			found = true
			out = physical.Expression{
				Type:           descriptor.OutputType,
				ExpressionType: physical.ExpressionTypeFunctionCall,
				FunctionCall: &physical.FunctionCall{
					Name:               fe.Name,
					Arguments:          arguments,
					FunctionDescriptor: descriptor,
				},
			}
		}
	}
	if !found {
	descriptorLoop2:
		for _, descriptor := range details.Descriptors {
			argTypes := argumentTypes
			if descriptor.Strict {
				argTypes = nonNullableArgumentTypes
			}
			if len(argTypes) != len(descriptor.ArgumentTypes) {
				continue
			}
			isMaybe := make([]bool, len(argTypes))
			for i := range argTypes {
				if rel := argTypes[i].Is(descriptor.ArgumentTypes[i]); rel < octosql.TypeRelationMaybe {
					continue descriptorLoop2
				} else if rel == octosql.TypeRelationMaybe {
					isMaybe[i] = true
				}
			}
			for i := range arguments {
				if isMaybe[i] {
					targetType := descriptor.ArgumentTypes[i]
					if descriptor.Strict {
						targetType = octosql.TypeSum(targetType, octosql.Null)
					}
					arguments[i] = physical.Expression{
						ExpressionType: physical.ExpressionTypeTypeAssertion,
						Type:           *octosql.TypeIntersection(targetType, arguments[i].Type),
						TypeAssertion: &physical.TypeAssertion{
							Expression: arguments[i],
							TargetType: targetType,
						},
					}
				}
			}
			found = true
			out = physical.Expression{
				Type:           descriptor.OutputType,
				ExpressionType: physical.ExpressionTypeFunctionCall,
				FunctionCall: &physical.FunctionCall{
					Name:               fe.Name,
					Arguments:          arguments,
					FunctionDescriptor: descriptor,
				},
			}
		}
	}

	if !found {
		argTypeNames := make([]string, len(arguments))
		for i := range argTypeNames {
			argTypeNames[i] = arguments[i].Type.String()
		}
		panic(fmt.Sprintf("unknown function: %s(%s)", fe.Name, strings.Join(argTypeNames, ", ")))
	}

	if out.FunctionCall.FunctionDescriptor.Strict {
		for i := range out.FunctionCall.Arguments {
			if octosql.Null.Is(out.FunctionCall.Arguments[i].Type) == octosql.TypeRelationIs {
				out.Type = octosql.TypeSum(out.Type, octosql.Null)
			}
		}
	}

	return out
}

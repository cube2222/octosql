package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

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

func (fe *FunctionExpression) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	arguments := make([]physical.Expression, len(fe.arguments))
	for i := range fe.arguments {
		arguments[i] = fe.arguments[i].Typecheck(ctx, env, logicalEnv)
	}

	descriptors := env.Functions[fe.name]
descriptorLoop:
	for _, descriptor := range descriptors {
		if len(arguments) != len(descriptor.ArgumentTypes) {
			continue
		}
		for i := range arguments {
			if arguments[i].Type.Is(descriptor.ArgumentTypes[i]) < octosql.TypeRelationIs {
				continue descriptorLoop
			}
		}
		return physical.Expression{
			Type:           descriptor.OutputType,
			ExpressionType: physical.ExpressionTypeFunctionCall,
			FunctionCall: &physical.FunctionCall{
				Name:      fe.name,
				Arguments: arguments,
			},
		}
	}
descriptorLoop2:
	for _, descriptor := range descriptors {
		if len(arguments) != len(descriptor.ArgumentTypes) {
			continue
		}
		isMaybe := make([]bool, len(arguments))
		for i := range arguments {
			if rel := arguments[i].Type.Is(descriptor.ArgumentTypes[i]); rel < octosql.TypeRelationMaybe {
				continue descriptorLoop2
			} else if rel == octosql.TypeRelationMaybe {
				isMaybe[i] = true
			}
		}
		for i := range arguments {
			if isMaybe[i] {
				arguments[i] = physical.Expression{
					ExpressionType: physical.ExpressionTypeTypeAssertion,
					Type:           descriptor.ArgumentTypes[i],
					TypeAssertion: &physical.TypeAssertion{
						Expression: arguments[i],
						TargetType: descriptor.ArgumentTypes[i],
					},
				}
			}
		}
		return physical.Expression{
			Type:           descriptor.OutputType,
			ExpressionType: physical.ExpressionTypeFunctionCall,
			FunctionCall: &physical.FunctionCall{
				Name:      fe.name,
				Arguments: arguments,
			},
		}
	}
	argTypeNames := make([]string, len(arguments))
	for i := range argTypeNames {
		argTypeNames[i] = arguments[i].Type.String()
	}
	panic(fmt.Sprintf("unknown function: %s(%s)", fe.name, strings.Join(argTypeNames, ", ")))
}

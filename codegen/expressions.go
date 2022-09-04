package codegen

import (
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func (g *CodeGenerator) Expression(ctx Context, expression physical.Expression) Value {
	switch expression.ExpressionType {
	case physical.ExpressionTypeVariable:
		value := ctx.VariableContext.GetValue(expression.Variable.Name)
		return *value
	case physical.ExpressionTypeConstant:
		switch expression.Constant.Value.TypeID {
		case octosql.TypeIDInt:
			return Value{
				Type:      octosql.Int,
				Reference: fmt.Sprintf("%d", expression.Constant.Value.Int),
			}
		case octosql.TypeIDBoolean:
			return Value{
				Type:      octosql.Boolean,
				Reference: fmt.Sprintf("%t", expression.Constant.Value.Boolean),
			}
		default:
			panic(fmt.Sprintf("implement me: %s", expression.Constant.Value.TypeID))
		}
	case physical.ExpressionTypeFunctionCall:
		var args []Value
		for i := range expression.FunctionCall.Arguments {
			args = append(args, g.Expression(ctx, expression.FunctionCall.Arguments[i]))
		}
		switch expression.FunctionCall.Name {
		// TODO: Move to descriptor.
		case "=":
			unique := g.Unique("equals")
			g.Printfln("%s := %s == %s", unique, args[0].Reference, args[1].Reference)
			return Value{
				Type:      octosql.Boolean,
				Reference: unique,
			}
		case "%":
			unique := g.Unique("mod")
			g.Printfln("%s := %s %% %s", unique, args[0].Reference, args[1].Reference)
			return Value{
				Type:      octosql.Int,
				Reference: unique,
			}
		default:
			panic("implement me")
		}
	default:
		panic("implement me")
	}
}

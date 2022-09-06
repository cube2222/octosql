package codegen

import (
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func (g *CodeGenerator) Expression(ctx Context, expression physical.Expression) Register {
	switch expression.ExpressionType {
	case physical.ExpressionTypeVariable:
		register := ctx.VariableContext.GetValue(expression.Variable.Name)
		return *register
	case physical.ExpressionTypeConstant:
		constant := g.DeclareVariable("constant", expression.Constant.Value.Type())
		switch expression.Constant.Value.TypeID {
		case octosql.TypeIDBoolean:
			g.Printfln("%s = %t", constant.AsType(octosql.TypeIDBoolean), expression.Constant.Value.Boolean)
		case octosql.TypeIDInt:
			g.Printfln("%s = %d", constant.AsType(octosql.TypeIDInt), expression.Constant.Value.Int)
		case octosql.TypeIDFloat:
			g.Printfln("%s = %f", constant.AsType(octosql.TypeIDFloat), expression.Constant.Value.Float)
		default:
			panic(fmt.Sprintf("implement me: %s", expression.Constant.Value.TypeID))
		}
		return constant
	case physical.ExpressionTypeFunctionCall:
		var args []Register
		for i := range expression.FunctionCall.Arguments {
			args = append(args, g.Expression(ctx, expression.FunctionCall.Arguments[i]))
		}
		switch expression.FunctionCall.Name {
		// TODO: Move to descriptor.
		case "=":
			unique := g.DeclareVariable("equals", octosql.Boolean)
			// TODO: Fixme different types.
			g.Printfln("%s = %s == %s", unique.AsType(octosql.TypeIDBoolean), args[0].DebugRawReference(), args[1].DebugRawReference())
			return unique
		case "%":
			unique := g.DeclareVariable("mod", octosql.Int)
			g.Printfln("%s = %s %% %s", unique.AsType(octosql.TypeIDInt), args[0].AsType(octosql.TypeIDInt), args[1].AsType(octosql.TypeIDInt))
			return unique
		case "+":
			unique := g.DeclareVariable("plus", octosql.Float)
			g.Printfln("%s = %s + %s", unique.AsType(octosql.TypeIDFloat), args[0].AsType(octosql.TypeIDFloat), args[1].AsType(octosql.TypeIDFloat))
			return unique
		case "*":
			unique := g.DeclareVariable("mul", octosql.Float)
			g.Printfln("%s = %s * %s", unique.AsType(octosql.TypeIDFloat), args[0].AsType(octosql.TypeIDFloat), args[1].AsType(octosql.TypeIDFloat))
			return unique
		case "len":
			unique := g.DeclareVariable("length", octosql.Int)
			// TODO: Fixme arrays and tuples.
			g.Printfln("%s = len(%s)", unique.AsType(octosql.TypeIDInt), args[0].AsType(octosql.TypeIDString))
			return unique
		default:
			panic("implement me")
		}
	default:
		panic("implement me")
	}
}

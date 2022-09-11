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
			g.SetVariable(constant, octosql.TypeIDBoolean, "%t", expression.Constant.Value.Boolean)
		case octosql.TypeIDInt:
			g.SetVariable(constant, octosql.TypeIDInt, "%d", expression.Constant.Value.Int)
		case octosql.TypeIDFloat:
			g.SetVariable(constant, octosql.TypeIDFloat, "%f", expression.Constant.Value.Float)
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
			g.SetVariable(unique, octosql.TypeIDBoolean, "%s == %s", args[0].DebugRawReference(), args[1].DebugRawReference())
			return unique
		case "%":
			unique := g.DeclareVariable("mod", octosql.Int)
			g.SetVariable(unique, octosql.TypeIDInt, "%s %% %s", args[0].AsType(octosql.TypeIDInt), args[1].AsType(octosql.TypeIDInt))
			return unique
		case "+":
			unique := g.DeclareVariable("plus", octosql.Float)
			g.SetVariable(unique, octosql.TypeIDFloat, "%s + %s", args[0].AsType(octosql.TypeIDFloat), args[1].AsType(octosql.TypeIDFloat))
			return unique
		case "*":
			unique := g.DeclareVariable("mul", octosql.Float)
			g.SetVariable(unique, octosql.TypeIDFloat, "%s * %s", args[0].AsType(octosql.TypeIDFloat), args[1].AsType(octosql.TypeIDFloat))
			return unique
		case "len":
			unique := g.DeclareVariable("length", octosql.Int)
			// TODO: Fixme arrays and tuples.
			g.SetVariable(unique, octosql.TypeIDInt, "len(%s)", args[0].AsType(octosql.TypeIDString))
			return unique
		default:
			panic("implement me")
		}
	default:
		panic("implement me")
	}
}

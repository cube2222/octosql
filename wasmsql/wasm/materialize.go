package wasm

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func MaterializeNode(ctx context.Context, node physical.Node, env physical.Environment) (Node, error) {
	switch node.NodeType {
	case physical.NodeTypeMap:
		source, err := MaterializeNode(ctx, node.Map.Source, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize source: %w", err)
		}
		exprs := make([]Expression, len(node.Map.Expressions))
		for i, expr := range node.Map.Expressions {
			materializedExpr, err := MaterializeExpr(ctx, expr, env.WithRecordSchema(node.Map.Source.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize expression: %w", err)
			}
			exprs[i] = materializedExpr
		}
		return &Map{
			Source: source,
			Exprs:  exprs,
			Schema: node.Schema,
		}, nil
	case physical.NodeTypeTableValuedFunction:
		switch node.TableValuedFunction.Name {
		case "range":
			// TODO: Support expressions as arguments of the range node.
			return &Range{
				LocalName: node.Schema.Fields[0].Name,
				Start:     int32(node.TableValuedFunction.Arguments["start"].Expression.Expression.Constant.Value.Int),
				End:       int32(node.TableValuedFunction.Arguments["end"].Expression.Expression.Constant.Value.Int),
			}, nil
		default:
			panic("invalid table valued function")
		}
	case physical.NodeTypeInMemoryRecords:
		return &InMemoryRecords{
			Records: node.InMemoryRecords.Records,
			Schema:  node.Schema,
		}, nil
	case physical.NodeTypeDatasource:
		uniqueToColname := make(map[string]string)
		for k, v := range node.Datasource.VariableMapping {
			uniqueToColname[v] = strings.TrimPrefix(k, node.Datasource.Alias+".")
		}
		predicatesOriginalNames := physical.RenameExpressionSliceRecordVariables(uniqueToColname, node.Datasource.Predicates)

		fieldsOriginalNames := make([]physical.SchemaField, len(node.Schema.Fields))
		for i := range node.Schema.Fields {
			fieldsOriginalNames[i] = physical.SchemaField{
				Name: uniqueToColname[node.Schema.Fields[i].Name],
				Type: node.Schema.Fields[i].Type,
			}
		}
		schemaOriginalNames := physical.NewSchema(fieldsOriginalNames, node.Schema.TimeField)

		materialized, err := node.Datasource.DatasourceImplementation.Materialize(ctx, env, schemaOriginalNames, predicatesOriginalNames)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize datasource: %w", err)
		}

		return &Datasource{
			Schema: node.Schema,
			Source: materialized,
		}, nil
	default:
		panic(fmt.Sprintf("invalid node type: %s", node.NodeType))
	}
}

func MaterializeExpr(ctx context.Context, expr physical.Expression, env physical.Environment) (Expression, error) {
	switch expr.ExpressionType {
	case physical.ExpressionTypeConstant:
		switch expr.Constant.Value.TypeID {
		case octosql.TypeIDInt:
			return &ConstantInteger{Value: int32(expr.Constant.Value.Int)}, nil
		case octosql.TypeIDFloat:
			return &ConstantFloat{Value: float32(expr.Constant.Value.Float)}, nil
		default:
			panic(fmt.Sprintf("invalid constant type: %s", expr.Constant.Value.TypeID))
		}
	case physical.ExpressionTypeVariable:
		return &ReadLocal{Name: expr.Variable.Name}, nil
	case physical.ExpressionTypeFunctionCall:
		switch expr.FunctionCall.Name {
		case "+":
			left, err := MaterializeExpr(ctx, expr.FunctionCall.Arguments[0], env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize left argument: %w", err)
			}
			right, err := MaterializeExpr(ctx, expr.FunctionCall.Arguments[1], env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize right argument: %w", err)
			}
			// TODO: Support addition of other types.

			switch expr.FunctionCall.Arguments[0].Type.TypeID {
			case octosql.TypeIDInt:
				// TODO: Support mixed-type additions (like time + duration).
				return &AddIntegers{Left: left, Right: right}, nil
			case octosql.TypeIDFloat:
				return &AddFloats{Left: left, Right: right}, nil
			default:
				panic(fmt.Sprintf("invalid type for addition: %s", expr.FunctionCall.Arguments[0].Type.TypeID))
			}
		default:
			args := make([]Expression, len(expr.FunctionCall.Arguments))
			for i := range expr.FunctionCall.Arguments {
				arg, err := MaterializeExpr(ctx, expr.FunctionCall.Arguments[i], env)
				if err != nil {
					return nil, fmt.Errorf("couldn't materialize argument: %w", err)
				}
				args[i] = arg
			}
			return &CallBuiltinFunc{Name: expr.FunctionCall.Name, Arguments: args}, nil
		}
	default:
		panic(fmt.Sprintf("invalid expression type: %s", expr.ExpressionType))
	}
}

package physical

import (
	"fmt"

	"github.com/cube2222/octosql/helpers/graph"
	"github.com/cube2222/octosql/octosql"
)

func DescribeNode(node Node, withTypeInfo bool) *graph.Node {
	var out *graph.Node
	switch node.NodeType {
	case NodeTypeDatasource:
		out = graph.NewNode(node.Datasource.Name)
		if len(node.Datasource.Predicates) > 0 {
			out.AddChild("predicate", DescribeExpr(Expression{
				Type:           octosql.Boolean,
				ExpressionType: ExpressionTypeAnd,
				And: &And{
					Arguments: node.Datasource.Predicates,
				},
			}, withTypeInfo))
		}

	case NodeTypeDistinct:
		out = graph.NewNode("distinct")
		out.AddChild("source", DescribeNode(node.Distinct.Source, withTypeInfo))

	case NodeTypeFilter:
		out = graph.NewNode("filter")
		out.AddChild("predicate", DescribeExpr(node.Filter.Predicate, withTypeInfo))
		out.AddChild("source", DescribeNode(node.Filter.Source, withTypeInfo))

	case NodeTypeGroupBy:
		out = graph.NewNode("group by")

		for i := range node.GroupBy.Aggregates {
			out.AddChild(node.GroupBy.Aggregates[i].Name, DescribeExpr(node.GroupBy.AggregateExpressions[i], withTypeInfo))
		}

		out.AddChild("key", DescribeExpr(Expression{
			ExpressionType: ExpressionTypeTuple,
			Tuple: &Tuple{
				Arguments: node.GroupBy.Key,
			},
		}, withTypeInfo))
		out.AddChild("source", DescribeNode(node.GroupBy.Source, withTypeInfo))

	case NodeTypeStreamJoin:
		out = graph.NewNode("join")
		out.AddChild("left", DescribeNode(node.StreamJoin.Left, withTypeInfo))
		out.AddChild("right", DescribeNode(node.StreamJoin.Right, withTypeInfo))
		out.AddChild("left_key", DescribeExpr(Expression{
			ExpressionType: ExpressionTypeTuple,
			Tuple: &Tuple{
				Arguments: node.StreamJoin.LeftKey,
			},
		}, withTypeInfo))
		out.AddChild("right_key", DescribeExpr(Expression{
			ExpressionType: ExpressionTypeTuple,
			Tuple: &Tuple{
				Arguments: node.StreamJoin.RightKey,
			},
		}, withTypeInfo))

	case NodeTypeLookupJoin:
		out = graph.NewNode("lookup join")
		out.AddChild("source", DescribeNode(node.LookupJoin.Source, withTypeInfo))
		out.AddChild("joined", DescribeNode(node.LookupJoin.Joined, withTypeInfo))

	case NodeTypeMap:
		out = graph.NewNode("map")

		for i := range node.Map.Expressions {
			out.AddChild(node.Schema.Fields[i].Name, DescribeExpr(node.Map.Expressions[i], withTypeInfo))
		}

		out.AddChild("source", DescribeNode(node.Map.Source, withTypeInfo))

	case NodeTypeOrderBy:
		out = graph.NewNode("sort")

		for i := range node.OrderBy.Key {
			if node.OrderBy.DirectionMultipliers[i] == 1 {
				out.AddChild("asc", DescribeExpr(node.OrderBy.Key[i], withTypeInfo))
			} else {
				out.AddChild("desc", DescribeExpr(node.OrderBy.Key[i], withTypeInfo))
			}
		}

		out.AddChild("source", DescribeNode(node.OrderBy.Source, withTypeInfo))

	case NodeTypeRequalifier:
		out = graph.NewNode("requalifier")
		out.AddField("new qualifier", node.Requalifier.Qualifier)
		out.AddChild("source", DescribeNode(node.Requalifier.Source, withTypeInfo))

	case NodeTypeTableValuedFunction:
		out = graph.NewNode(node.TableValuedFunction.Name)
		for name, value := range node.TableValuedFunction.Arguments {
			switch value.TableValuedFunctionArgumentType {
			case TableValuedFunctionArgumentTypeExpression:
				out.AddChild(name, DescribeExpr(value.Expression.Expression, withTypeInfo))

			case TableValuedFunctionArgumentTypeTable:
				out.AddChild(name, DescribeNode(value.Table.Table, withTypeInfo))

			case TableValuedFunctionArgumentTypeDescriptor:
				descriptor := graph.NewNode("descriptor")
				descriptor.AddField("value", value.Descriptor.Descriptor)
				out.AddChild(name, descriptor)

			default:
				panic(fmt.Sprintf("unrecognized table valued function argument type: %v", value.TableValuedFunctionArgumentType))
			}
		}
	case NodeTypeUnnest:
		out = graph.NewNode("unnest")
		out.AddField("field", node.Unnest.Field)
		out.AddChild("source", DescribeNode(node.Unnest.Source, withTypeInfo))

	default:
		panic("unexhaustive node type match")
	}

	if withTypeInfo {
		typeNode := graph.NewNode("schema")
		for i := range node.Schema.Fields {
			name := node.Schema.Fields[i].Name
			if i == node.Schema.TimeField {
				name = "*" + name
			}
			typeNode.AddField(node.Schema.Fields[i].Name, node.Schema.Fields[i].Type.String())
		}
		typeNode.AddChild("", out)
		out = typeNode
	}

	return out
}

func DescribeExpr(expr Expression, withTypeInfo bool) *graph.Node {
	var out *graph.Node
	switch expr.ExpressionType {
	case ExpressionTypeVariable:
		out = graph.NewNode("variable")
		out.AddField("name", expr.Variable.Name)
		out.AddField("is_level_0", fmt.Sprintf("%t", expr.Variable.IsLevel0))

	case ExpressionTypeConstant:
		out = graph.NewNode("constant")
		out.AddField("value", expr.Constant.Value.String())

	case ExpressionTypeFunctionCall:
		out = graph.NewNode("function")
		out.AddField(expr.FunctionCall.Name, "")
		for i := range expr.FunctionCall.Arguments {
			out.AddChild(fmt.Sprintf("arg_%d", i), DescribeExpr(expr.FunctionCall.Arguments[i], withTypeInfo))
		}

	case ExpressionTypeAnd:
		out = graph.NewNode("and")
		for i := range expr.And.Arguments {
			out.AddChild(fmt.Sprintf("arg_%d", i), DescribeExpr(expr.And.Arguments[i], withTypeInfo))
		}

	case ExpressionTypeOr:
		out = graph.NewNode("or")
		for i := range expr.Or.Arguments {
			out.AddChild(fmt.Sprintf("arg_%d", i), DescribeExpr(expr.Or.Arguments[i], withTypeInfo))
		}

	case ExpressionTypeQueryExpression:
		out = graph.NewNode("subquery")
		out.AddChild("source", DescribeNode(expr.QueryExpression.Source, withTypeInfo))

	case ExpressionTypeCoalesce:
		out = graph.NewNode("coalesce")
		for i := range expr.Coalesce.Arguments {
			out.AddChild(fmt.Sprintf("arg_%d", i), DescribeExpr(expr.Coalesce.Arguments[i], withTypeInfo))
		}

	case ExpressionTypeTuple:
		out = graph.NewNode("tuple")
		for i := range expr.Tuple.Arguments {
			out.AddChild(fmt.Sprintf("arg_%d", i), DescribeExpr(expr.Tuple.Arguments[i], withTypeInfo))
		}

	case ExpressionTypeTypeAssertion:
		out = graph.NewNode("type assertion")
		out.AddField("type", expr.TypeAssertion.TargetType.String())
		out.AddChild("value", DescribeExpr(expr.TypeAssertion.Expression, withTypeInfo))

	default:
		panic("unexhaustive expression type match")
	}

	if withTypeInfo {
		typeNode := graph.NewNode(expr.Type.String())
		typeNode.AddChild("", out)
		out = typeNode
	}

	return out
}

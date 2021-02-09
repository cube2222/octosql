package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

type Transformers struct {
	NodeTransformer       func(node Node) Node
	ExpressionTransformer func(expr Expression) Expression
}

func (t *Transformers) TransformNode(node Node) Node {
	var out Node
	switch node.NodeType {
	case NodeTypeDatasource:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Datasource: &Datasource{
				Name:                     node.Datasource.Name,
				DatasourceImplementation: node.Datasource.DatasourceImplementation,
			},
		}
	case NodeTypeFilter:
		// TODO: I think we'd like a function which automatically does type deduction again on each transformation?
		// Naah, we can do that based on the currently executing transformation.
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Filter: &Filter{
				Source:    t.TransformNode(node.Filter.Source),
				Predicate: t.TransformExpr(node.Filter.Predicate),
			},
		}
	case NodeTypeGroupBy:
		aggregates := make([]Aggregate, len(node.GroupBy.Aggregates))
		copy(aggregates, node.GroupBy.Aggregates)
		aggregateExpressions := make([]Expression, len(node.GroupBy.AggregateExpressions))
		for i := range node.GroupBy.AggregateExpressions {
			aggregateExpressions[i] = t.TransformExpr(node.GroupBy.AggregateExpressions[i])
		}
		key := make([]Expression, len(node.GroupBy.Key))
		for i := range node.GroupBy.Key {
			key[i] = t.TransformExpr(node.GroupBy.Key[i])
		}

		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			GroupBy: &GroupBy{
				Source:               t.TransformNode(node.Filter.Source),
				Aggregates:           aggregates,
				AggregateExpressions: aggregateExpressions,
				Key:                  key,
				Trigger:              node.GroupBy.Trigger,
			},
		}
	case NodeTypeJoin:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Join: &Join{
				Left:  t.TransformNode(node.Join.Left),
				Right: t.TransformNode(node.Join.Right),
				On:    t.TransformExpr(node.Join.On),
			},
		}
	case NodeTypeMap:
		expressions := make([]Expression, len(node.Map.Expressions))
		for i := range node.Map.Expressions {
			expressions[i] = t.TransformExpr(node.Map.Expressions[i])
		}
		aliases := make([]*string, len(node.Map.Aliases))
		copy(aliases, node.Map.Aliases)

		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Map: &Map{
				Source:      t.TransformNode(node.Filter.Source),
				Expressions: expressions,
				Aliases:     aliases,
			},
		}
	case NodeTypeRequalifier:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Requalifier: &Requalifier{
				Source:    t.TransformNode(node.Filter.Source),
				Qualifier: node.Requalifier.Qualifier,
			},
		}
	case NodeTypeTableValuedFunction:
		arguments := make(map[string]TableValuedFunctionArgument)
		for name, arg := range node.TableValuedFunction.Arguments {
			switch arg.TableValuedFunctionArgumentType {
			case TableValuedFunctionArgumentTypeExpression:
				arguments[name] = TableValuedFunctionArgument{
					TableValuedFunctionArgumentType: arg.TableValuedFunctionArgumentType,
					Expression: &TableValuedFunctionArgumentExpression{
						Expression: t.TransformExpr(arg.Expression.Expression),
					},
				}
			case TableValuedFunctionArgumentTypeTable:
				arguments[name] = TableValuedFunctionArgument{
					TableValuedFunctionArgumentType: arg.TableValuedFunctionArgumentType,
					Table: &TableValuedFunctionArgumentTable{
						Table: t.TransformNode(arg.Table.Table),
					},
				}
			case TableValuedFunctionArgumentTypeDescriptor:
				arguments[name] = TableValuedFunctionArgument{
					TableValuedFunctionArgumentType: arg.TableValuedFunctionArgumentType,
					Descriptor: &TableValuedFunctionArgumentDescriptor{
						Descriptor: arg.Descriptor.Descriptor,
					},
				}
			default:
				panic("unexhaustive table valued function argument type match")
			}
		}

		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			TableValuedFunction: &TableValuedFunction{
				Name:               node.TableValuedFunction.Name,
				Arguments:          arguments,
				FunctionDescriptor: node.TableValuedFunction.FunctionDescriptor,
			},
		}
	default:
		panic("unexhaustive node type match")
	}

	if t.NodeTransformer != nil {
		out = t.NodeTransformer(out)
	}

	return out
}

func (t *Transformers) TransformExpr(expr Expression) Expression {
	var out Expression
	switch expr.ExpressionType {
	case ExpressionTypeVariable:
		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			Variable: &Variable{
				Name:     expr.Variable.Name,
				IsLevel0: expr.Variable.IsLevel0,
			},
		}
	case ExpressionTypeConstant:
		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			Constant: &Constant{
				Value: expr.Constant.Value,
			},
		}
	case ExpressionTypeFunctionCall:
		arguments := make([]Expression, len(expr.FunctionCall.Arguments))
		for i := range expr.FunctionCall.Arguments {
			arguments[i] = t.TransformExpr(expr.FunctionCall.Arguments[i])
		}

		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			FunctionCall: &FunctionCall{
				Name:               expr.FunctionCall.Name,
				Arguments:          arguments,
				FunctionDescriptor: expr.FunctionCall.FunctionDescriptor,
			},
		}
	case ExpressionTypeAnd:
		arguments := make([]Expression, len(expr.And.Arguments))
		for i := range expr.And.Arguments {
			arguments[i] = t.TransformExpr(expr.And.Arguments[i])
		}

		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			And: &And{
				Arguments: arguments,
			},
		}
	case ExpressionTypeOr:
		arguments := make([]Expression, len(expr.Or.Arguments))
		for i := range expr.Or.Arguments {
			arguments[i] = t.TransformExpr(expr.Or.Arguments[i])
		}

		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			Or: &Or{
				Arguments: arguments,
			},
		}
	case ExpressionTypeTypeAssertion:
		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			TypeAssertion: &TypeAssertion{
				Expression: t.TransformExpr(expr.TypeAssertion.Expression),
				TargetType: expr.TypeAssertion.TargetType,
			},
		}
	default:
		panic("unexhaustive expression type match")
	}

	if t.ExpressionTransformer != nil {
		out = t.ExpressionTransformer(out)
	}

	return out
}

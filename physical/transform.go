package physical

type Transformers struct {
	NodeTransformer       func(node Node) Node
	ExpressionTransformer func(expr Expression) Expression
}

// TODO: This should probably push down and make available the environment.
func (t *Transformers) TransformNode(node Node) Node {
	var out Node
	switch node.NodeType {
	case NodeTypeDatasource:
		pushedDownPredicates := make([]Expression, len(node.Datasource.Predicates))
		for i := range node.Datasource.Predicates {
			pushedDownPredicates[i] = t.TransformExpr(node.Datasource.Predicates[i])
		}
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Datasource: &Datasource{
				Name:                     node.Datasource.Name,
				Alias:                    node.Datasource.Alias,
				DatasourceImplementation: node.Datasource.DatasourceImplementation,
				Predicates:               pushedDownPredicates,
				VariableMapping:          node.Datasource.VariableMapping,
			},
		}
	case NodeTypeDistinct:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Distinct: &Distinct{
				Source: t.TransformNode(node.Distinct.Source),
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
				Source:               t.TransformNode(node.GroupBy.Source),
				Aggregates:           aggregates,
				AggregateExpressions: aggregateExpressions,
				Key:                  key,
				KeyEventTimeIndex:    node.GroupBy.KeyEventTimeIndex,
				Trigger:              node.GroupBy.Trigger,
			},
		}
	case NodeTypeStreamJoin:
		leftKey := make([]Expression, len(node.StreamJoin.LeftKey))
		for i := range node.StreamJoin.LeftKey {
			leftKey[i] = t.TransformExpr(node.StreamJoin.LeftKey[i])
		}
		rightKey := make([]Expression, len(node.StreamJoin.RightKey))
		for i := range node.StreamJoin.RightKey {
			rightKey[i] = t.TransformExpr(node.StreamJoin.RightKey[i])
		}

		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			StreamJoin: &StreamJoin{
				Left:     t.TransformNode(node.StreamJoin.Left),
				Right:    t.TransformNode(node.StreamJoin.Right),
				LeftKey:  leftKey,
				RightKey: rightKey,
			},
		}
	case NodeTypeLookupJoin:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			LookupJoin: &LookupJoin{
				Source: t.TransformNode(node.LookupJoin.Source),
				Joined: t.TransformNode(node.LookupJoin.Joined),
			},
		}
	case NodeTypeMap:
		expressions := make([]Expression, len(node.Map.Expressions))
		for i := range node.Map.Expressions {
			expressions[i] = t.TransformExpr(node.Map.Expressions[i])
		}

		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Map: &Map{
				Source:      t.TransformNode(node.Map.Source),
				Expressions: expressions,
			},
		}
	case NodeTypeOrderBy:
		keyExprs := make([]Expression, len(node.OrderBy.Key))
		for i := range node.OrderBy.Key {
			keyExprs[i] = t.TransformExpr(node.OrderBy.Key[i])
		}
		directionMultipliers := make([]int, len(node.OrderBy.DirectionMultipliers))
		copy(directionMultipliers, node.OrderBy.DirectionMultipliers)

		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			OrderBy: &OrderBy{
				Source:               t.TransformNode(node.OrderBy.Source),
				Key:                  keyExprs,
				DirectionMultipliers: directionMultipliers,
			},
		}
	case NodeTypeRequalifier:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Requalifier: &Requalifier{
				Source:    t.TransformNode(node.Requalifier.Source),
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
	case NodeTypeUnnest:
		out = Node{
			Schema:   node.Schema,
			NodeType: node.NodeType,
			Unnest: &Unnest{
				Source: t.TransformNode(node.Unnest.Source),
				Field:  node.Unnest.Field,
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
	case ExpressionTypeQueryExpression:
		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			QueryExpression: &QueryExpression{
				Source: t.TransformNode(expr.QueryExpression.Source),
			},
		}
	case ExpressionTypeCoalesce:
		arguments := make([]Expression, len(expr.Coalesce.Arguments))
		for i := range expr.Coalesce.Arguments {
			arguments[i] = t.TransformExpr(expr.Coalesce.Arguments[i])
		}

		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			Coalesce: &Coalesce{
				Arguments: arguments,
			},
		}
	case ExpressionTypeTuple:
		arguments := make([]Expression, len(expr.Tuple.Arguments))
		for i := range expr.Tuple.Arguments {
			arguments[i] = t.TransformExpr(expr.Tuple.Arguments[i])
		}

		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			Tuple: &Tuple{
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
	case ExpressionTypeCast:
		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			Cast: &Cast{
				Expression:   t.TransformExpr(expr.Cast.Expression),
				TargetTypeID: expr.Cast.TargetTypeID,
			},
		}
	case ExpressionTypeObjectFieldAccess:
		out = Expression{
			Type:           expr.Type,
			ExpressionType: expr.ExpressionType,
			ObjectFieldAccess: &ObjectFieldAccess{
				Object: t.TransformExpr(expr.ObjectFieldAccess.Object),
				Field:  expr.ObjectFieldAccess.Field,
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

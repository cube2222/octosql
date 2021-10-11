package optimizer

import (
	"github.com/cube2222/octosql/octosql"
	. "github.com/cube2222/octosql/physical"
)

func PushDownFilterPredicatesIntoLookupJoinBranch(node Node) (Node, bool) {
	changed := false
	t := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeFilter {
				return node
			}
			if node.Filter.Source.NodeType != NodeTypeLookupJoin {
				return node
			}
			changed = true

			sourceSchema := node.Filter.Source.LookupJoin.Source.Schema
			joinedSchema := node.Filter.Source.LookupJoin.Joined.Schema

			filterPredicates := node.Filter.Predicate.SplitByAnd()
			var pushedDownSource, pushedDownJoined []Expression

			for i := range filterPredicates {
				variablesUsed := filterPredicates[i].VariablesUsed()
				if !usesVariablesFromSchema(joinedSchema, variablesUsed) {
					pushedDownSource = append(pushedDownSource, filterPredicates[i])
				} else {
					pushedDownJoined = append(pushedDownJoined, filterPredicates[i])
				}
			}

			joinSourceSource := node.Filter.Source.LookupJoin.Source
			if len(pushedDownSource) > 0 {
				joinSourceSource = Node{
					Schema:   joinSourceSource.Schema,
					NodeType: NodeTypeFilter,
					Filter: &Filter{
						Source: joinSourceSource,
						Predicate: Expression{
							Type:           octosql.Boolean,
							ExpressionType: ExpressionTypeAnd,
							And: &And{
								Arguments: pushedDownSource,
							},
						},
					},
				}
			}
			joinSourceJoined := node.Filter.Source.LookupJoin.Joined
			if len(pushedDownJoined) > 0 {
				joinSourceJoined = Node{
					Schema:   joinSourceJoined.Schema,
					NodeType: NodeTypeFilter,
					Filter: &Filter{
						Source: joinSourceJoined,
						Predicate: transformVariablesFromSchemaIntoNonLevel0(
							sourceSchema,
							Expression{
								Type:           octosql.Boolean,
								ExpressionType: ExpressionTypeAnd,
								And: &And{
									Arguments: pushedDownJoined,
								},
							},
						),
					},
				}
			}

			out := Node{
				Schema:   node.Filter.Source.Schema,
				NodeType: NodeTypeLookupJoin,
				LookupJoin: &LookupJoin{
					Source: joinSourceSource,
					Joined: joinSourceJoined,
				},
			}

			return out
		},
	}
	output := t.TransformNode(node)

	if changed {
		return output, true
	} else {
		return node, false
	}
}

func transformVariablesFromSchemaIntoNonLevel0(schema Schema, expr Expression) Expression {
	t := Transformers{
		ExpressionTransformer: func(expr Expression) Expression {
			if expr.ExpressionType != ExpressionTypeVariable {
				return expr
			}
			for _, field := range schema.Fields {
				if field.Name == expr.Variable.Name {
					expr.Variable.IsLevel0 = false
					return expr
				}
			}
			return expr
		},
	}
	return t.TransformExpr(expr)
}

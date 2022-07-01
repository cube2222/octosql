package optimizer

import (
	"github.com/cube2222/octosql/octosql"
	. "github.com/cube2222/octosql/physical"
)

func PushDownFilterPredicatesIntoStreamJoinKey(node Node) (Node, bool) {
	changed := false
	t := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeFilter {
				return node
			}
			if node.Filter.Source.NodeType != NodeTypeStreamJoin {
				return node
			}
			leftSchema := node.Filter.Source.StreamJoin.Left.Schema
			rightSchema := node.Filter.Source.StreamJoin.Right.Schema

			filterPredicates := node.Filter.Predicate.SplitByAnd()
			var stayedAbove, leftKeyAdd, rightKeyAdd []Expression

			for i := range filterPredicates {
				if filterPredicates[i].ExpressionType != ExpressionTypeFunctionCall {
					stayedAbove = append(stayedAbove, filterPredicates[i])
					continue
				}
				if filterPredicates[i].FunctionCall.Name != "=" {
					stayedAbove = append(stayedAbove, filterPredicates[i])
					continue
				}
				firstPart := filterPredicates[i].FunctionCall.Arguments[0]
				secondPart := filterPredicates[i].FunctionCall.Arguments[1]
				firstPartVariables := firstPart.VariablesUsed()
				firstPartUsesLeftVariables := UsesVariablesFromSchema(leftSchema, firstPartVariables)
				firstPartUsesRightVariables := UsesVariablesFromSchema(rightSchema, firstPartVariables)
				secondPartVariables := secondPart.VariablesUsed()
				secondPartUsesLeftVariables := UsesVariablesFromSchema(leftSchema, secondPartVariables)
				secondPartUsesRightVariables := UsesVariablesFromSchema(rightSchema, secondPartVariables)

				if firstPartUsesLeftVariables && !firstPartUsesRightVariables &&
					!secondPartUsesLeftVariables && secondPartUsesRightVariables {
					leftKeyAdd = append(leftKeyAdd, firstPart)
					rightKeyAdd = append(rightKeyAdd, secondPart)
				} else if !firstPartUsesLeftVariables && firstPartUsesRightVariables &&
					secondPartUsesLeftVariables && !secondPartUsesRightVariables {
					rightKeyAdd = append(rightKeyAdd, firstPart)
					leftKeyAdd = append(leftKeyAdd, secondPart)
				} else {
					stayedAbove = append(stayedAbove, filterPredicates[i])
				}
			}

			if len(stayedAbove) == len(filterPredicates) {
				return node
			}
			changed = true

			out := Node{
				Schema:   node.Filter.Source.Schema,
				NodeType: NodeTypeStreamJoin,
				StreamJoin: &StreamJoin{
					LeftKey:  append(node.Filter.Source.StreamJoin.LeftKey, leftKeyAdd...),
					RightKey: append(node.Filter.Source.StreamJoin.RightKey, rightKeyAdd...),
					Left:     node.Filter.Source.StreamJoin.Left,
					Right:    node.Filter.Source.StreamJoin.Right,
				},
			}
			if len(stayedAbove) > 0 {
				out = Node{
					Schema:   out.Schema,
					NodeType: NodeTypeFilter,
					Filter: &Filter{
						Predicate: Expression{
							Type:           octosql.Boolean,
							ExpressionType: ExpressionTypeAnd,
							And: &And{
								Arguments: stayedAbove,
							},
						},
						Source: out,
					},
				}
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

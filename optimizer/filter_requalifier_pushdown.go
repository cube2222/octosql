package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

func PushDownFilterUnderRequalifier(node Node) (Node, bool) {
	changed := false
	t := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeFilter {
				return node
			}
			if node.Filter.Source.NodeType != NodeTypeRequalifier {
				return node
			}
			changed = true

			oldSourceSchemaFields := node.Filter.Source.Schema.Fields
			newSourceSchemaFields := node.Filter.Source.Requalifier.Source.Schema.Fields

			renamer := Transformers{
				ExpressionTransformer: func(expr Expression) Expression {
					if expr.ExpressionType != ExpressionTypeVariable {
						return expr
					}
					if !expr.Variable.IsLevel0 {
						return expr
					}
					for i, field := range oldSourceSchemaFields {
						if VariableNameMatchesField(expr.Variable.Name, field.Name) {
							return Expression{
								Type:           expr.Type,
								ExpressionType: ExpressionTypeVariable,
								Variable: &Variable{
									Name:     newSourceSchemaFields[i].Name,
									IsLevel0: true,
								},
							}
						}
					}
					panic("variable didn't match any source schema field after typecheck")
				},
			}
			newPredicate := renamer.TransformExpr(node.Filter.Predicate)

			return Node{
				Schema:   node.Filter.Source.Schema,
				NodeType: NodeTypeRequalifier,
				Requalifier: &Requalifier{
					Qualifier: node.Filter.Source.Requalifier.Qualifier,
					Source: Node{
						Schema:   node.Filter.Source.Requalifier.Source.Schema,
						NodeType: NodeTypeFilter,
						Filter: &Filter{
							Predicate: newPredicate,
							Source:    node.Filter.Source.Requalifier.Source,
						},
					},
				},
			}
		},
	}
	output := t.TransformNode(node)

	if changed {
		return output, true
	} else {
		return node, false
	}
}

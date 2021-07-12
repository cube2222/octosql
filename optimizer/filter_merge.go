package optimizer

import (
	"github.com/cube2222/octosql/octosql"
	. "github.com/cube2222/octosql/physical"
)

func MergeFilters(node Node) (Node, bool) {
	changed := false
	t := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeFilter {
				return node
			}
			if node.Filter.Source.NodeType != NodeTypeFilter {
				return node
			}
			changed = true

			return Node{
				Schema:   node.Filter.Source.Schema,
				NodeType: NodeTypeFilter,
				Filter: &Filter{
					Predicate: Expression{
						Type:           octosql.TypeSum(node.Filter.Predicate.Type, node.Filter.Source.Filter.Predicate.Type),
						ExpressionType: ExpressionTypeAnd,
						And: &And{
							Arguments: append(node.Filter.Predicate.SplitByAnd(), node.Filter.Source.Filter.Predicate.SplitByAnd()...),
						},
					},
					Source: node.Filter.Source.Filter.Source,
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

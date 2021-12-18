package optimizer

import (
	"github.com/cube2222/octosql/octosql"
	. "github.com/cube2222/octosql/physical"
)

func PushDownFilterPredicatesToDatasource(node Node) (Node, bool) {
	changed := false
	t := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeFilter {
				return node
			}
			if node.Filter.Source.NodeType != NodeTypeDatasource {
				return node
			}

			filterPredicates := node.Filter.Predicate.SplitByAnd()
			alreadyPushedDown := node.Filter.Source.Datasource.Predicates

			newFilterPredicates, newPushedDownPredicates, curChanged := node.Filter.Source.Datasource.PushDownPredicates(filterPredicates, alreadyPushedDown)
			if !curChanged {
				return node
			}
			changed = true

			out := Node{
				Schema:   node.Filter.Source.Schema,
				NodeType: NodeTypeDatasource,
				Datasource: &Datasource{
					Name:                     node.Filter.Source.Datasource.Name,
					Alias:                    node.Filter.Source.Datasource.Alias,
					DatasourceImplementation: node.Filter.Source.Datasource.DatasourceImplementation,
					Predicates:               newPushedDownPredicates,
					VariableMapping:          node.Filter.Source.Datasource.VariableMapping,
				},
			}
			if len(newFilterPredicates) > 0 {
				out = Node{
					Schema:   node.Schema,
					NodeType: NodeTypeFilter,
					Filter: &Filter{
						Predicate: Expression{
							Type:           octosql.Boolean,
							ExpressionType: ExpressionTypeAnd,
							And: &And{
								Arguments: newFilterPredicates,
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

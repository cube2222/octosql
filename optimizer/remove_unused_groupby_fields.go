package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

func RemoveUnusedGroupByNonKeyFields(node Node) (Node, bool) {
	changed := false

	fields := getNonTimeFieldGroupByFields(node)
	for i := range fields {
		if !isUsed(fields[i], node) {
			node = removeGroupByField(fields[i], node)
			changed = true
		}
	}

	return node, changed
}

func getNonTimeFieldGroupByFields(node Node) []string {
	fields := make([]string, 0)
	fieldCollector := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeGroupBy {
				return node
			}
			for i, field := range node.Schema.Fields {
				if i < len(node.GroupBy.Key) {
					continue
				}
				if i == node.Schema.TimeField {
					continue
				}
				fields = append(fields, field.Name)
			}

			return node
		},
	}
	fieldCollector.TransformNode(node)

	return fields
}

func removeGroupByField(field string, node Node) Node {
	fieldRemover := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeGroupBy {
				return node
			}
			index := -1
			for i := range node.Schema.Fields {
				if node.Schema.Fields[i].Name == field {
					index = i
				}
			}
			if index == -1 {
				return node
			}

			aggregateIndex := index - len(node.GroupBy.Key)

			node.Schema.Fields = append(node.Schema.Fields[:index], node.Schema.Fields[index+1:]...)
			node.GroupBy.AggregateExpressions = append(node.GroupBy.AggregateExpressions[:aggregateIndex], node.GroupBy.AggregateExpressions[aggregateIndex+1:]...)
			node.GroupBy.Aggregates = append(node.GroupBy.Aggregates[:aggregateIndex], node.GroupBy.Aggregates[aggregateIndex+1:]...)
			if node.Schema.TimeField > index {
				node.Schema.TimeField--
			}

			return node
		},
	}
	return fieldRemover.TransformNode(node)
}

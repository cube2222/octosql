package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

func RemoveUnusedDatasourceFields(node Node) (Node, bool) {
	changed := false

	fields := getNonTimeFieldDatasourceFields(node)
	for i := range fields {
		if !isUsed(fields[i], node) {
			node = removeUnusedDatasourceField(fields[i], node)
			node = removeFieldFromPassers(fields[i], node)
			changed = true
		}
	}

	return node, changed
}

func getNonTimeFieldDatasourceFields(node Node) []string {
	fields := make([]string, 0)
	fieldCollector := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeDatasource {
				return node
			}
			for i, field := range node.Schema.Fields {
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

func removeUnusedDatasourceField(field string, node Node) Node {
	fieldRemover := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeDatasource {
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

			node.Schema.Fields = append(node.Schema.Fields[:index], node.Schema.Fields[index+1:]...)
			if node.Schema.TimeField > index {
				node.Schema.TimeField--
			}

			return node
		},
	}
	return fieldRemover.TransformNode(node)
}

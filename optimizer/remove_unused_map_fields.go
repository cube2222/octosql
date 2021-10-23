package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

func RemoveUnusedMapFields(node Node) (Node, bool) {
	changed := false

	fields := getNonTimeFieldMapFields(node)
	for i := range fields {
		if !isUsed(fields[i], node) {
			node = removeMapField(fields[i], node)
			changed = true
		}
	}

	return node, changed
}

func getNonTimeFieldMapFields(node Node) []string {
	fields := make([]string, 0)
	fieldCollector := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeMap {
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

func isUsed(field string, node Node) bool {
	// Check if it's an output field. If it is, then it's printed, so it's used.
	for i := range node.Schema.Fields {
		if node.Schema.Fields[i].Name == field {
			return true
		}
	}

	used := false

	usageChecker := Transformers{
		ExpressionTransformer: func(expr Expression) Expression {
			switch expr.ExpressionType {
			case ExpressionTypeVariable:
				if expr.Variable.Name == field {
					used = true
				}
			case ExpressionTypeQueryExpression:
				// Top-level of query expression is used.
				for i := range expr.QueryExpression.Source.Schema.Fields {
					if expr.QueryExpression.Source.Schema.Fields[i].Name == field {
						used = true
					}
				}
			}

			return expr
		},
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeTableValuedFunction {
				return node
			}
			for _, arg := range node.TableValuedFunction.Arguments {
				if arg.TableValuedFunctionArgumentType != TableValuedFunctionArgumentTypeDescriptor {
					continue
				}
				if arg.Descriptor.Descriptor == field {
					used = true
				}
			}

			return node
		},
	}
	usageChecker.TransformNode(node)

	return used
}

func removeMapField(field string, node Node) Node {
	fieldRemover := Transformers{
		NodeTransformer: func(node Node) Node {
			if node.NodeType != NodeTypeMap {
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
			node.Map.Expressions = append(node.Map.Expressions[:index], node.Map.Expressions[index+1:]...)
			if node.Schema.TimeField > index {
				node.Schema.TimeField--
			}

			return node
		},
	}
	return fieldRemover.TransformNode(node)
}

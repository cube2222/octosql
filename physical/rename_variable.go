package physical

func RenameVariables(oldToNew map[string]string, node Node) Node {
	t := Transformers{
		ExpressionTransformer: func(expr Expression) Expression {
			// TODO: Think about how this affects TBV argument table mapping.
			// It should properly rename descriptors.
			if expr.ExpressionType == ExpressionTypeVariable {
				if newName, ok := oldToNew[expr.Variable.Name]; ok {
					expr.Variable.Name = newName
				}
			}
			return expr
		},
	}
	return t.TransformNode(node)
}

func RenameVariablesExpr(oldToNew map[string]string, expr Expression) Expression {
	t := Transformers{
		ExpressionTransformer: func(expr Expression) Expression {
			// TODO: Think about how this affects TBV argument table mapping.
			// It should properly rename descriptors.
			if expr.ExpressionType == ExpressionTypeVariable {
				if newName, ok := oldToNew[expr.Variable.Name]; ok {
					expr.Variable.Name = newName
				}
			}
			return expr
		},
	}
	return t.TransformExpr(expr)
}

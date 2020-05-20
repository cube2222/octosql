package sql

import (
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type PlaceholderMap interface {
	AddPlaceholder(physical.Expression) string
	MaterializePlaceholders(*physical.MaterializationContext) ([]execution.Expression, error)
	GetAlias() string
}

func FormulaToSQL(formula physical.Formula, pm PlaceholderMap) string {
	switch formula := formula.(type) {
	case *physical.And:
		left := FormulaToSQL(formula.Left, pm)

		right := FormulaToSQL(formula.Right, pm)

		return fmt.Sprintf("%s AND %s", parenthesize(left), parenthesize(right))

	case *physical.Or:
		left := FormulaToSQL(formula.Left, pm)

		right := FormulaToSQL(formula.Right, pm)

		return fmt.Sprintf("%s OR %s", parenthesize(left), parenthesize(right))

	case *physical.Not:
		child := FormulaToSQL(formula.Child, pm)

		return fmt.Sprintf("NOT %s", parenthesize(child))
	case *physical.Constant:
		if formula.Value {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case *physical.Predicate:
		left := expressionToSQL(formula.Left, pm)

		right := expressionToSQL(formula.Right, pm)

		relationString := relationToSQL(formula.Relation)

		return fmt.Sprintf("%s %s %s", parenthesize(left), relationString, parenthesize(right))
	default:
		panic("Unknown type of physical.Formula")
	}
}

func parenthesize(s string) string {
	return fmt.Sprintf("(%s)", s)
}

func expressionToSQL(expression physical.Expression, pm PlaceholderMap) string {
	switch expression := expression.(type) {
	case *physical.Variable: // if it's a variable, then check if it's a column name for alias
		if expression.Name.Source() == pm.GetAlias() {
			return expression.Name.String()
		}

		placeholder := pm.AddPlaceholder(expression)
		return placeholder
	default:
		placeholder := pm.AddPlaceholder(expression)
		return placeholder
	}
}

func relationToSQL(rel physical.Relation) string {
	switch rel {
	case physical.Equal:
		return "="
	case physical.NotEqual:
		return "<>"
	case physical.MoreThan:
		return ">"
	case physical.LessThan:
		return "<"
	case physical.In:
		return "IN"
	case physical.Like:
		return "LIKE"
	case physical.GreaterEqual:
		return ">="
	case physical.LessEqual:
		return "<="
	default:
		panic("Invalid physical relation")
	}
}

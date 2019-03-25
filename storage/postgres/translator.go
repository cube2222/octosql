package postgres

import (
	"fmt"
	"github.com/cube2222/octosql/physical"
)

type Aliases struct {
	PlaceholderToExpression map[string]physical.Expression
	Counter                 int
	Alias                   string
}

func NewAliases(alias string) *Aliases {
	return &Aliases{
		PlaceholderToExpression: make(map[string]physical.Expression),
		Counter:                 1,
		Alias:                   alias,
	}
}

func RelationToSQL(rel physical.Relation) string {
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
	default:
		panic("Invalid physical relation")
	}
}

func (aliases *Aliases) newPlaceholder() string {
	str := fmt.Sprintf("$%d", aliases.Counter)
	aliases.Counter++
	return str
}

func ExpressionToSQL(expression physical.Expression, aliases *Aliases) string {
	switch expression := expression.(type) {
	case *physical.Variable:
		if expression.Name.Source() == aliases.Alias {
			return expression.Name.String()
		}

		placeholder := aliases.newPlaceholder()
		aliases.PlaceholderToExpression[placeholder] = expression

		return placeholder

	default:
		placeholder := aliases.newPlaceholder()
		aliases.PlaceholderToExpression[placeholder] = expression

		return placeholder
	}
}

func FormulaToSQL(formula physical.Formula, aliases *Aliases) string {
	switch formula := formula.(type) {
	case *physical.And:
		left := FormulaToSQL(formula.Left, aliases)

		right := FormulaToSQL(formula.Right, aliases)

		return fmt.Sprintf("%s AND %s", parenthesize(left), parenthesize(right))

	case *physical.Or:
		left := FormulaToSQL(formula.Left, aliases)

		right := FormulaToSQL(formula.Right, aliases)

		return fmt.Sprintf("%s OR %s", parenthesize(left), parenthesize(right))

	case *physical.Not:
		child := FormulaToSQL(formula.Child, aliases)

		return fmt.Sprintf("NOT %s", parenthesize(child))
	case *physical.Constant:
		if formula.Value {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case *physical.Predicate:
		left := ExpressionToSQL(formula.Left, aliases)

		right := ExpressionToSQL(formula.Right, aliases)

		relationString := RelationToSQL(formula.Relation)

		return fmt.Sprintf("%s %s %s", parenthesize(left), relationString, parenthesize(right))
	default:
		panic("Unknown type of physical.Formula")
	}
}

func parenthesize(str string) string {
	return fmt.Sprintf("(%s)", str)
}

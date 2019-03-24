package postgres

import (
	"strconv"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

func parenthesize(text string) string {
	return "(" + text + ")"
}

type Aliases struct {
	PlaceholderToVariable map[string]octosql.VariableName
	Counter               int
	TableAlias            string
}

func NewAliases(tableAlias string) *Aliases {
	return &Aliases{
		PlaceholderToVariable: make(map[string]octosql.VariableName),
		Counter:               1,
		TableAlias:            tableAlias,
	}
}

func (aliases *Aliases) isAliasedForTable(variable octosql.VariableName) bool {
	typed := variable.String()
	if !strings.Contains(typed, ".") {
		return false
	}

	splitString := strings.Split(typed, ".")
	alias := splitString[0]
	return alias == aliases.TableAlias
}


func (aliases* Aliases) alias (variable octosql.VariableName) string  {
	if aliases.isAliasedForTable(variable) {
		return variable.String()
	}

	placeholder := "$" + strconv.Itoa(aliases.Counter)
	aliases.Counter++

	aliases.PlaceholderToVariable[placeholder] = variable

	return placeholder
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

func ExpressionToSQL(expression physical.Expression, aliases *Aliases) (string, error) {
	switch expression := expression.(type) {
	case *physical.AliasedExpression: //TODO: ?
		panic("Implement aliased expr")
	case *physical.NodeExpression:
		panic("NodeExpression not supported in translation to SQL")
	case *physical.Variable:
		return aliases.alias(expression.Name), nil


	default:
		panic("Unknown expression")
	}
}

func FormulaToSQL(formula physical.Formula, aliases *Aliases) (string, error) {
	switch formula := formula.(type) {
	case *physical.And:
		left, err := FormulaToSQL(formula.Left, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform left subformula to string")
		}

		right, err := FormulaToSQL(formula.Right, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform right subformula to string")
		}

		return parenthesize(left) + " AND " + parenthesize(right), nil

	case *physical.Or:
		left, err := FormulaToSQL(formula.Left, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform left subformula to string in OR")
		}

		right, err := FormulaToSQL(formula.Right, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform right subformula to string in OR")
		}

		return parenthesize(left) + " OR " + parenthesize(right), nil

	case *physical.Not:
		child, err := FormulaToSQL(formula.Child, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform child subformula to string")
		}

		return "NOT " + parenthesize(child), nil
	case *physical.Constant:
		if formula.Value {
			return "TRUE", nil
		} else {
			return "FALSE", nil
		}
	case *physical.Predicate:
		left, err := ExpressionToSQL(formula.Left, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform left expression to string")
		}

		right, err := ExpressionToSQL(formula.Right, aliases)
		if err != nil {
			return "", errors.Wrap(err, "couldn't transform right expression to string")
		}

		relationString := RelationToSQL(formula.Relation)

		return parenthesize(left) + " " + relationString + " " + parenthesize(right), nil
	default:
		panic("Unknown type of physical.Formula")
	}
}

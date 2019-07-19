package mysql

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

//A structure that stores the relation $n -> expression
//that will be later used to put specific values into a SQL query
type aliases struct {
	PlaceholderToExpression []physical.Expression
	Alias                   string
}

func newAliases(alias string) *aliases {
	return &aliases{
		PlaceholderToExpression: make([]physical.Expression, 0),
		Alias:                   alias,
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

func expressionToSQL(expression physical.Expression, aliases *aliases) string {
	switch expression := expression.(type) {
	case *physical.Variable: //if it's a variable, then check if it's a column name for alias
		if expression.Name.Source() == aliases.Alias {
			return expression.Name.String()
		}
		//if not, or we are not a variable, then create a new placeholder and assign the expression to it
		aliases.PlaceholderToExpression = append(aliases.PlaceholderToExpression, expression)
	default:
		aliases.PlaceholderToExpression = append(aliases.PlaceholderToExpression, expression)
	}
	return "?"
}

func formulaToSQL(formula physical.Formula, aliases *aliases) string {
	switch formula := formula.(type) {
	case *physical.And:
		left := formulaToSQL(formula.Left, aliases)

		right := formulaToSQL(formula.Right, aliases)

		return fmt.Sprintf("%s AND %s", parenthesize(left), parenthesize(right))

	case *physical.Or:
		left := formulaToSQL(formula.Left, aliases)

		right := formulaToSQL(formula.Right, aliases)

		return fmt.Sprintf("%s OR %s", parenthesize(left), parenthesize(right))

	case *physical.Not:
		child := formulaToSQL(formula.Child, aliases)

		return fmt.Sprintf("NOT %s", parenthesize(child))
	case *physical.Constant:
		if formula.Value {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case *physical.Predicate:
		left := expressionToSQL(formula.Left, aliases)

		right := expressionToSQL(formula.Right, aliases)

		relationString := relationToSQL(formula.Relation)

		return fmt.Sprintf("%s %s %s", parenthesize(left), relationString, parenthesize(right))
	default:
		panic("Unknown type of physical.Formula")
	}
}

func parenthesize(str string) string {
	return fmt.Sprintf("(%s)", str)
}

//materializes the values in the map so that one can later call EvaluateExpression on them
func (aliases *aliases) materializeAliases(matCtx *physical.MaterializationContext) ([]execution.Expression, error) {
	result := make([]execution.Expression, len(aliases.PlaceholderToExpression))

	ctx := context.Background()

	for placeholder, expression := range aliases.PlaceholderToExpression {
		exec, err := expression.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize expression in materializeAliases")
		}

		result[placeholder] = exec
	}

	return result, nil
}

package mysql

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

// A structure that stores the relation ? -> expression
// that will be later used to put specific values into a SQL query
type mySQLPlaceholders struct {
	PlaceholderToExpression []physical.Expression
	Alias                   string
}

func newMySQLPlaceholders(alias string) *mySQLPlaceholders {
	return &mySQLPlaceholders{
		PlaceholderToExpression: make([]physical.Expression, 0),
		Alias:                   alias,
	}
}

func (mps *mySQLPlaceholders) AddPlaceholder(expression physical.Expression) string {
	mps.PlaceholderToExpression = append(mps.PlaceholderToExpression, expression)

	return "?"
}

// Materializes the values in the map so that one can later call EvaluateExpression on them
func (mps *mySQLPlaceholders) MaterializePlaceholders(matCtx *physical.MaterializationContext) ([]execution.Expression, error) {
	result := make([]execution.Expression, len(mps.PlaceholderToExpression))

	ctx := context.Background()

	for index, expression := range mps.PlaceholderToExpression {
		materializedExpression, err := expression.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize expression in MaterializePlaceholders")
		}

		result[index] = materializedExpression
	}

	return result, nil
}

func (mps *mySQLPlaceholders) GetAlias() string {
	return mps.Alias
}

package postgres

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

//A structure that stores the relation $n -> expression
//that will be later used to put specific values into a SQL query
type postgresPlaceholders struct {
	PlaceholderToExpression map[string]physical.Expression
	Counter                 int
	Alias                   string
}

func newPostgresPlaceholders(alias string) *postgresPlaceholders {
	return &postgresPlaceholders{
		PlaceholderToExpression: make(map[string]physical.Expression),
		Counter:                 1,
		Alias:                   alias,
	}
}

func (pms *postgresPlaceholders) AddPlaceholder(physical.Expression) string {
	placeholder := fmt.Sprintf("$%d", pms.Counter)
	pms.Counter++

	return placeholder
}

//materializes the values in the map so that one can later call EvaluateExpression on them
func (pms *postgresPlaceholders) MaterializePlaceholders(matCtx *physical.MaterializationContext) ([]execution.Expression, error) {
	result := make([]execution.Expression, pms.Counter-1)

	ctx := context.Background()

	for i := 1; i < pms.Counter; i++ {
		placeholder := fmt.Sprintf("$%d", i)

		expression, ok := pms.PlaceholderToExpression[placeholder]
		if !ok {
			return nil, errors.Errorf("couldn'template get variable name for placeholder %s", placeholder)
		}

		exec, err := expression.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn'template materialize expression in Postgres' MaterializePlaceholders")
		}

		result[i-1] = exec
	}

	return result, nil
}

func (pms *postgresPlaceholders) GetAlias() string {
	return pms.Alias
}

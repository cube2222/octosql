package redis

import (
	"context"
	"fmt"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

func EvaluateExpression(expression physical.Expression, variables octosql.Variables) (octosql.VariableName, error) {
	execExpression, err := expression.Materialize(context.Background())
	if err != nil {
		return "", errors.Wrap(err, "couldn't materialize expression")
	}

	exprValue, err := execExpression.ExpressionValue(variables)
	if err != nil {
		return "", errors.Wrap(err, "couldn't get expression value")
	}

	switch exprValue := exprValue.(type) {
	case string:
		return octosql.NewVariableName(exprValue), nil

	default:
		return "", errors.Errorf("wrong expression value for redis database")
	}
}

func GetAllKeys(filter physical.Formula, variables octosql.Variables, key octosql.VariableName, alias string) (octosql.Variables, error) {
	switch formula := filter.(type) {
	case *physical.And:
		leftKeys, err := GetAllKeys(formula.Left, variables, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get left keys")
		}
		rightKeys, err := GetAllKeys(formula.Right, variables, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get right keys")
		}

		if leftKeys.Equal(rightKeys) {
			return leftKeys, nil
		}
		return nil, errors.Errorf("left and right keys are not equal")

	case *physical.Or:
		leftKeys, err := GetAllKeys(formula.Left, variables, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get left keys")
		}

		rightKeys, err := GetAllKeys(formula.Right, variables, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get right keys")
		}

		leftKeys, err = leftKeys.MergeWith(rightKeys)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't merge left and right keys")
		}
		return leftKeys, nil

	case *physical.Predicate:
		leftKeyValue, err := EvaluateExpression(formula.Left, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get value of left expression")
		}
		rightKeyValue, err := EvaluateExpression(formula.Right, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get value of right expression")
		}

		if leftKeyValue.String() != fmt.Sprintf("%s.%s", alias, key) {
			if rightKeyValue.String() != fmt.Sprintf("%s.%s", alias, key) {
				return nil, errors.Errorf("neither of predicates expressions represents key identifier")
			}

			return octosql.NewVariables(map[octosql.VariableName]interface{}{
				leftKeyValue: nil, // TODO - co ?
			}), nil
		}

		return octosql.NewVariables(map[octosql.VariableName]interface{}{
			rightKeyValue: nil, // TODO - co ?
		}), nil

	default:
		return nil, errors.Errorf("wrong formula type for redis database")
	}
}

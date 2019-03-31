package redis

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type KeyFormula interface {
	GetAllKeys(variables octosql.Variables) (octosql.Variables, error)
}

type And struct {
	left, right KeyFormula
}

func NewAnd(left, right KeyFormula) *And {
	return &And{
		left:  left,
		right: right,
	}
}

func (f *And) GetAllKeys(variables octosql.Variables) (octosql.Variables, error) {
	leftKeys, err := f.left.GetAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from left KeyFormula")
	}

	rightKeys, err := f.right.GetAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from right KeyFormula")
	}

	resultKeys := make(map[octosql.VariableName]interface{})
	for k := range leftKeys {
		if val, ok := rightKeys[k]; ok {
			resultKeys[k] = val
		}
	}

	return resultKeys, nil
}

type Or struct {
	left, right KeyFormula
}

func NewOr(left, right KeyFormula) *Or {
	return &Or{
		left:  left,
		right: right,
	}
}

func (f *Or) GetAllKeys(variables octosql.Variables) (octosql.Variables, error) {
	leftKeys, err := f.left.GetAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from left KeyFormula")
	}

	rightKeys, err := f.right.GetAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from right KeyFormula")
	}

	leftKeys, err = leftKeys.MergeWithNoConflicts(rightKeys)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't merge left and right keys")
	}

	return leftKeys, nil
}

type Equal struct {
	child execution.Expression
}

func NewEqual(child execution.Expression) *Equal {
	return &Equal{
		child: child,
	}
}

func (f *Equal) GetAllKeys(variables octosql.Variables) (octosql.Variables, error) {
	exprValue, err := f.child.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get child expression value")
	}

	switch exprValue := exprValue.(type) {
	case string:
		return octosql.NewVariables(
			map[octosql.VariableName]interface{}{
				octosql.NewVariableName(exprValue): nil,
			},
		), nil

	default:
		return nil, errors.Errorf("wrong expression value for a key in redis database")
	}
}

func NewKeyFormula(formula physical.Formula, keyAlias octosql.VariableName) (KeyFormula, error) {
	if formula == nil { // there was no formula, so we want to scan whole redis database (no error returned)
		return nil, nil
	}

	switch formula := formula.(type) {
	case *physical.And:
		leftFormula, err := NewKeyFormula(formula.Left, keyAlias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		rightFormula, err := NewKeyFormula(formula.Right, keyAlias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		return NewAnd(leftFormula, rightFormula), nil

	case *physical.Or:
		leftFormula, err := NewKeyFormula(formula.Left, keyAlias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		rightFormula, err := NewKeyFormula(formula.Right, keyAlias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		return NewOr(leftFormula, rightFormula), nil

	case *physical.Predicate:
		materializedLeft, err := formula.Left.Materialize(context.Background())
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize left expression")
		}
		materializedRight, err := formula.Right.Materialize(context.Background())
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize right expression")
		}

		if !IsExpressionKeyAlias(materializedLeft, keyAlias) {
			if !IsExpressionKeyAlias(materializedRight, keyAlias) {
				return nil, errors.Errorf("neither of predicates expressions represents key identifier")
			}

			return NewEqual(materializedLeft), nil
		}

		return NewEqual(materializedRight), nil

	default:
		return nil, errors.Errorf("wrong formula type for redis database")
	}
}

func IsExpressionKeyAlias(expression execution.Expression, keyAlias octosql.VariableName) bool {
	// TODO - veeery ugly
	switch expression := expression.(type) {
	case execution.NamedExpression: // TODO - it should be case execution.Variable but for any reason it's not working :<
		if expression.Name() == keyAlias {
			return true
		}

	default:
		return false
	}

	return false
}

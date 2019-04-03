package redis

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

// Formula build from physical.Formula, so that it accepts formulas for redis database
// Also, getAllKeys returns all keys, that will be subject of HGetAll
type KeyFormula interface {
	getAllKeys(variables octosql.Variables) (octosql.Variables, error)
}

// Just as with logical constant
// getAllKeys returns empty map for 'false' (so that AND have empty intersection and doesn't have any impact on OR's union)
// for 'true' it also returns empty map (no effect on OR), but we need a type condition in AND struct
type Constant struct {
	value bool
}

func NewConstant(value bool) *Constant {
	return &Constant{
		value: value,
	}
}

func (f *Constant) getAllKeys(variables octosql.Variables) (octosql.Variables, error) {
	return octosql.NoVariables(), nil
}

// Just like logical operator, the form is like: (formula1 AND formula2)
// getAllKeys returns intersection of 2 key sets
type And struct {
	left, right KeyFormula
}

func NewAnd(left, right KeyFormula) *And {
	return &And{
		left:  left,
		right: right,
	}
}

func (f *And) getAllKeys(variables octosql.Variables) (octosql.Variables, error) {
	leftKeys, err := f.left.getAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from left KeyFormula")
	}

	rightKeys, err := f.right.getAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from right KeyFormula")
	}

	switch leftType := f.left.(type) {
	case *Constant:
		if leftType.value {
			return rightKeys, nil // left formula is TRUE
		}
		return leftKeys, nil // empty map otherwise

	default: // left formula is not constant, we need to check right formula
		switch rightType := f.right.(type) {
		case *Constant:
			if rightType.value {
				return leftKeys, nil // same as above
			}
			return rightKeys, nil

		default: // neither of children formulas are constant - we return their intersection
			resultKeys := make(map[octosql.VariableName]interface{})
			for k := range leftKeys {
				if val, ok := rightKeys[k]; ok {
					resultKeys[k] = val
				}
			}

			return resultKeys, nil
		}
	}
}

// Just like logical operator, the form is like: (formula1 OR formula2)
// getAllKeys returns union of 2 key sets
type Or struct {
	left, right KeyFormula
}

func NewOr(left, right KeyFormula) *Or {
	return &Or{
		left:  left,
		right: right,
	}
}

func (f *Or) getAllKeys(variables octosql.Variables) (octosql.Variables, error) {
	leftKeys, err := f.left.getAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from left KeyFormula")
	}

	rightKeys, err := f.right.getAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from right KeyFormula")
	}

	leftKeys, err = leftKeys.MergeWithNoConflicts(rightKeys)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't merge left and right keys")
	}

	return leftKeys, nil
}

// The equivalent form is: ("key" = child), where "key" is alias for extracted key from redis database
// getAllKeys returns expression value of 'child' (only if it's of string type - the only acceptable in redis)
type Equal struct {
	child execution.Expression
}

func NewEqual(child execution.Expression) *Equal {
	return &Equal{
		child: child,
	}
}

func (f *Equal) getAllKeys(variables octosql.Variables) (octosql.Variables, error) {
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

func NewKeyFormula(formula physical.Formula, key, alias string) (KeyFormula, error) {
	switch formula := formula.(type) {
	case *physical.And:
		leftFormula, err := NewKeyFormula(formula.Left, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		rightFormula, err := NewKeyFormula(formula.Right, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from right formula")
		}

		return NewAnd(leftFormula, rightFormula), nil

	case *physical.Or:
		leftFormula, err := NewKeyFormula(formula.Left, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		rightFormula, err := NewKeyFormula(formula.Right, key, alias)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		return NewOr(leftFormula, rightFormula), nil

	case *physical.Predicate:
		if !IsExpressionKeyAlias(formula.Left, key, alias) {
			if !IsExpressionKeyAlias(formula.Right, key, alias) {
				return nil, errors.Errorf("neither of predicates expressions represents key identifier")
			}

			materializedLeft, err := formula.Left.Materialize(context.Background())
			if err != nil {
				return nil, errors.Wrap(err, "couldn't materialize left expression")
			}

			return NewEqual(materializedLeft), nil
		}

		materializedRight, err := formula.Right.Materialize(context.Background())
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize right expression")
		}

		return NewEqual(materializedRight), nil

	case *physical.Constant:
		return NewConstant(formula.Value), nil

	default:
		return nil, errors.Errorf("wrong formula type for redis database")
	}
}

func IsExpressionKeyAlias(expression physical.Expression, key, alias string) bool {
	switch expression := expression.(type) {
	case *physical.Variable:
		if expression.Name.Name() == key && expression.Name.Source() == alias {
			return true
		}

	default:
		return false
	}

	return false
}

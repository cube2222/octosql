package execution

import (
	"reflect"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Relation interface {
	Apply(variables octosql.Variables, left, right Expression) (bool, error)
}

type Equal struct {
}

func NewEqual() Relation {
	return &Equal{}
}

func (rel *Equal) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	// TODO: HAX fixme
	leftValue, err := left.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in more than")
	}
	rightValue, err := right.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in more than")
	}

	return reflect.DeepEqual(leftValue, rightValue), nil
}

type NotEqual struct {
}

func NewNotEqual() Relation {
	return &NotEqual{}
}

func (rel *NotEqual) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	panic("implement me")
}

type MoreThan struct {
}

func NewMoreThan() Relation {
	return &MoreThan{}
}

func (rel *MoreThan) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	allowed := []octosql.Datatype{octosql.DatatypeInt, octosql.DatatypeFloat32, octosql.DatatypeFloat64}
	leftValue, err := left.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in more than")
	}
	rightValue, err := right.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in more than")
	}

	if octosql.OneOf(getType(leftValue), allowed) && octosql.OneOf(getType(rightValue), allowed) {
		// TODO: HAX fixme
		var leftValueTyped float64
		var rightValueTyped float64
		switch getType(leftValue) {
		case octosql.DatatypeInt:
			leftValueTyped = float64(leftValue.(int))
		case octosql.DatatypeFloat32:
			leftValueTyped = float64(leftValue.(float32))
		case octosql.DatatypeFloat64:
			leftValueTyped = leftValue.(float64)
		}
		switch getType(rightValue) {
		case octosql.DatatypeInt:
			rightValueTyped = float64(rightValue.(int))
		case octosql.DatatypeFloat32:
			rightValueTyped = float64(rightValue.(float32))
		case octosql.DatatypeFloat64:
			rightValueTyped = rightValue.(float64)
		}

		return leftValueTyped > rightValueTyped, nil
	}
	return false, errors.Errorf(
		"invalid operands to more_than %v and %v with types %v and %v",
		leftValue, rightValue, getType(leftValue), getType(rightValue))
}

type LessThan struct {
}

func NewLessThan() Relation {
	return &LessThan{}
}

func (rel *LessThan) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	panic("implement me")
}

type Like struct {
}

func NewLike() Relation {
	return &Like{}
}

func (rel *Like) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	panic("implement me")
}

type In struct {
}

func NewIn() Relation {
	return &In{}
}

func (rel *In) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	panic("implement me")
}

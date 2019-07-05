package execution

import (
	"reflect"
	"regexp"
	"time"

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
	leftValue, err := left.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in equal")
	}
	rightValue, err := right.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in equal")
	}
	if leftValue == nil || rightValue == nil {
		if leftValue == nil && rightValue == nil {
			return true, nil
		}
		return false, nil
	}
	if reflect.TypeOf(leftValue).Kind() != reflect.TypeOf(rightValue).Kind() {
		return false, errors.Errorf(
			"invalid operands to equal %v and %v with types %v and %v",
			leftValue, rightValue, getType(leftValue), getType(rightValue))
	}

	return AreEqual(leftValue, rightValue), nil
}

type NotEqual struct {
}

func NewNotEqual() Relation {
	return &NotEqual{}
}

func (rel *NotEqual) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	equal, err := (*Equal).Apply(nil, variables, left, right)
	if err != nil {
		return false, errors.Wrap(err, "couldn't check equality")
	}
	return !equal, nil
}

type MoreThan struct {
}

func NewMoreThan() Relation {
	return &MoreThan{}
}

func (rel *MoreThan) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in more than")
	}
	rightValue, err := right.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in more than")
	}
	if leftValue == nil || rightValue == nil {
		return false, errors.Errorf("invalid null operand to more_than %v and %v", leftValue, rightValue)
	}
	if reflect.TypeOf(leftValue).Kind() != reflect.TypeOf(rightValue).Kind() {
		return false, errors.Errorf(
			"invalid operands to more_than %v and %v with types %v and %v",
			leftValue, rightValue, getType(leftValue), getType(rightValue))
	}

	switch leftValue := leftValue.(type) {
	case int:
		rightValue := rightValue.(int)
		return leftValue > rightValue, nil
	case float64:
		rightValue := rightValue.(float64)
		return leftValue > rightValue, nil
	case string:
		rightValue := rightValue.(string)
		return leftValue > rightValue, nil
	case time.Time:
		rightValue := rightValue.(time.Time)
		return leftValue.After(rightValue), nil
	}

	return false, errors.Errorf(
		"invalid operands to more_than %v and %v with types %v and %v, only int, float, string and time allowed",
		leftValue, rightValue, getType(leftValue), getType(rightValue))
}

type LessThan struct {
}

func NewLessThan() Relation {
	return &LessThan{}
}

func (rel *LessThan) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	more, err := (*MoreThan).Apply(nil, variables, right, left)
	if err != nil {
		return false, errors.Wrap(err, "couldn't check reverse more_than")
	}
	return more, nil
}

type GreaterEqual struct {
}

func NewGreaterEqual() Relation {
	return &GreaterEqual{}
}

func (rel *GreaterEqual) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	less, err := (*LessThan).Apply(nil, variables, left, right)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get less for greater_equal")
	}

	return !less, nil
}

type LessEqual struct {
}

func NewLessEqual() Relation {
	return &LessEqual{}
}

func (rel *LessEqual) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	more, err := (*MoreThan).Apply(nil, variables, left, right)
	if err != nil {
		return false, errors.Wrap(err, "coudln't get more for less_equal")
	}

	return !more, nil
}

type Like struct {
}

func NewLike() Relation {
	return &Like{}
}

func (rel *Like) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in LIKE")
	}
	rightValue, err := right.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in LIKE")
	}
	leftString, ok := leftValue.(string)
	if !ok {
		return false, errors.Errorf(
			"invalid operands to like %v and %v with types %v and %v, only string allowed",
			leftValue, rightValue, getType(leftValue), getType(rightValue))
	}
	rightString, ok := rightValue.(string)
	if !ok {
		return false, errors.Errorf(
			"invalid operands to like %v and %v with types %v and %v, only string allowed",
			leftValue, rightValue, getType(leftValue), getType(rightValue))
	}

	match, err := regexp.MatchString(rightString, leftString)
	if err != nil {
		return false, errors.Wrapf(err, "couldn't match string in like relation with pattern %v", rightString)
	}
	return match, nil
}

type In struct {
}

func NewIn() Relation {
	return &In{}
}

func (rel *In) Apply(variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in IN")
	}
	rightValue, err := right.ExpressionValue(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in IN")
	}

	switch set := rightValue.(type) {
	case []Record:
		for i := range set {
			if len(set[i].Fields()) == 1 {
				right := set[i].Value(set[i].Fields()[0].Name)
				if AreEqual(leftValue, right) {
					return true, nil
				}
				continue
			}
			switch leftValue.(type) {
			case *Record:
				return AreEqual(leftValue, &set[i]), nil
			case []interface{}:
				fields := set[i].Fields()
				values := make([]interface{}, len(fields))
				for i, field := range fields {
					values[i] = set[i].Value(field.Name)
				}
				return AreEqual(leftValue, values), nil
			}

		}
		return false, nil

	case *Record:
		switch leftValue.(type) {
		case *Record:
			return AreEqual(leftValue, rightValue), nil
		case []interface{}:
			fields := set.Fields()
			values := make([]interface{}, len(fields))
			for i, field := range fields {
				values[i] = set.Value(field.Name)
			}
			return AreEqual(leftValue, values), nil
		}

	case []interface{}:
		for i := range set {
			if AreEqual(leftValue, set[i]) {
				return true, nil
			}
		}
		return false, nil
	}
	return AreEqual(leftValue, rightValue), nil
}

package execution

import (
	"context"
	"regexp"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
)

type Relation interface {
	Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error)
}

type Equal struct {
}

func NewEqual() Relation {
	return &Equal{}
}

func (rel *Equal) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in equal")
	}
	rightValue, err := right.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in equal")
	}

	if leftValue.GetType() != rightValue.GetType() {
		return false, errors.Errorf(
			"invalid operands to equal %v and %v with types %v and %v",
			leftValue, rightValue, leftValue.GetType(), rightValue.GetType())
	}

	return octosql.AreEqual(leftValue, rightValue), nil
}

type NotEqual struct {
}

func NewNotEqual() Relation {
	return &NotEqual{}
}

func (rel *NotEqual) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	equal, err := (*Equal).Apply(nil, ctx, variables, left, right)
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

func (rel *MoreThan) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in more than")
	}
	rightValue, err := right.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in more than")
	}
	if leftValue.GetType() != rightValue.GetType() {
		return false, errors.Errorf(
			"invalid operands to more_than %v and %v with types %v and %v",
			leftValue, rightValue, leftValue.GetType(), rightValue.GetType())
	}

	switch leftValue.GetType() {
	case octosql.TypeInt:
		return leftValue.AsInt() > rightValue.AsInt(), nil
	case octosql.TypeFloat:
		return leftValue.AsFloat() > rightValue.AsFloat(), nil
	case octosql.TypeString:
		return leftValue.AsString() > rightValue.AsString(), nil
	case octosql.TypeTime:
		return leftValue.AsTime().After(rightValue.AsTime()), nil
	case octosql.TypeNull, octosql.TypePhantom, octosql.TypeBool, octosql.TypeDuration, octosql.TypeTuple, octosql.TypeObject:
		return false, errors.Errorf(
			"invalid operands to more_than %v and %v with types %v and %v, only int, float, string and time allowed",
			leftValue, rightValue, leftValue.GetType(), rightValue.GetType())
	}

	panic("unreachable")
}

type LessThan struct {
}

func NewLessThan() Relation {
	return &LessThan{}
}

func (rel *LessThan) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	more, err := (*MoreThan).Apply(nil, ctx, variables, right, left)
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

func (rel *GreaterEqual) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	less, err := (*LessThan).Apply(nil, ctx, variables, left, right)
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

func (rel *LessEqual) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	more, err := (*MoreThan).Apply(nil, ctx, variables, left, right)
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

func (rel *Like) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in LIKE")
	}
	rightValue, err := right.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in LIKE")
	}
	if leftValue.GetType() != octosql.TypeString {
		return false, errors.Errorf(
			"invalid operands to like %v and %v with types %v and %v, only string allowed",
			leftValue, rightValue, leftValue.GetType(), rightValue.GetType())
	}
	if rightValue.GetType() != octosql.TypeString {
		return false, errors.Errorf(
			"invalid operands to like %v and %v with types %v and %v, only string allowed",
			leftValue, rightValue, leftValue.GetType(), rightValue.GetType())
	}

	match, err := regexp.MatchString(rightValue.AsString(), leftValue.AsString())
	if err != nil {
		return false, errors.Wrapf(err, "couldn't match string in like relation with pattern %v", rightValue)
	}
	return match, nil
}

type In struct {
}

func NewIn() Relation {
	return &In{}
}

func (rel *In) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in IN")
	}
	rightValue, err := right.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in IN")
	}

	switch rightValue.GetType() {
	case octosql.TypeTuple:
		set := rightValue.AsSlice()
		for i := range set {
			if octosql.AreEqual(leftValue, set[i]) {
				return true, nil
			}
		}
		return false, nil

	default:
		return octosql.AreEqual(leftValue, rightValue), nil
	}
}

type NotIn struct {
}

func NewNotIn() Relation {
	return &NotIn{}
}

func (rel *NotIn) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	in, err := (*In).Apply(nil, ctx, variables, left, right)
	if err != nil {
		return false, errors.Wrap(err, "couldn't check containment")
	}
	return !in, nil
}

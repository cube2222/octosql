package execution

import (
	"context"
	"regexp"
	"strings"

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
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
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
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
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
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
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

const likeEscape = '\\'
const likeAny = '_'
const likeAll = '%'

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
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
	}
	if rightValue.GetType() != octosql.TypeString {
		return false, errors.Errorf(
			"invalid operands to like %v and %v with types %v and %v, only string allowed",
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
	}

	patternString, err := likePatternToRegexp(rightValue.AsString())
	if err != nil {
		return false, errors.Wrapf(err, "couldn't transform LIKE pattern %v to regexp", patternString)
	}

	match, err := regexp.MatchString(patternString, leftValue.AsString())
	if err != nil {
		return false, errors.Wrapf(err, "couldn't match string in like relation with pattern %v", rightValue)
	}
	return match, nil
}

//we assume that the escape character is '\'
func likePatternToRegexp(pattern string) (string, error) {
	var sb strings.Builder
	sb.WriteRune('^') // match start

	escaping := false // was the character previously seen an escaping \

	for _, r := range pattern {
		if escaping { // escaping \, _ and % is legal (we just write . or .*), otherwise an error occurs
			if r != likeAny && r != likeAll && r != likeEscape {
				return "", errors.Errorf("escaping invalid character in LIKE pattern: %v", r)
			}

			escaping = false
			sb.WriteRune(r)

			if r == likeEscape {
				// since _ and % don't need to be escaped in regexp we just replace \_ with _
				// but \ needs to be replaced in both, so we need to write an additional \
				sb.WriteRune(likeEscape)
			}
		} else {
			if r == likeEscape { // if we find an escape sequence we just handle it in the next step
				escaping = true
			} else if r == likeAny { // _ transforms to . (any character)
				sb.WriteRune('.')
			} else if r == likeAll { // % transforms to .* (any string)
				sb.WriteString(".*")
			} else if needsEscaping(r) { // escape characters that might break the regexp
				sb.WriteRune('\\')
				sb.WriteRune(r)
			} else { // just write everything else
				sb.WriteRune(r)
			}
		}
	}

	sb.WriteRune('$') // match end

	if escaping {
		return "", errors.New("pattern ends with an escape character that doesn't escape anything")
	}

	return sb.String(), nil
}

func needsEscaping(r rune) bool {
	return r == '+' ||
		r == '?' ||
		r == '(' ||
		r == ')' ||
		r == '{' ||
		r == '}' ||
		r == '[' ||
		r == ']' ||
		r == '^' ||
		r == '$' ||
		r == '.'
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

type Regexp struct {
}

func NewRegexp() Relation {
	return &Regexp{}
}

func (rel *Regexp) Apply(ctx context.Context, variables octosql.Variables, left, right Expression) (bool, error) {
	leftValue, err := left.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of left operator in REGEXP")
	}
	rightValue, err := right.ExpressionValue(ctx, variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get value of right operator in REGEXP")
	}
	if leftValue.GetType() != octosql.TypeString {
		return false, errors.Errorf(
			"invalid operands to regexp %v and %v with types %v and %v, only string allowed",
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
	}
	if rightValue.GetType() != octosql.TypeString {
		return false, errors.Errorf(
			"invalid operands to regexp %v and %v with types %v and %v, only string allowed",
			leftValue.Show(), rightValue.Show(), leftValue.GetType(), rightValue.GetType())
	}

	match, err := regexp.MatchString(rightValue.AsString(), leftValue.AsString())
	if err != nil {
		return false, errors.Wrapf(err, "couldn't match string in regexp relation with pattern %v", rightValue)
	}
	return match, nil
}

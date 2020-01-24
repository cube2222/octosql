package redis

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

// This ENUM says whether our KeyFormula returned just Variables (DefaultKeys) or True /Ł False (only constants)
// It is used only when And structure decides what to return based on children results
type resultType int

const (
	DefaultKeys resultType = iota
	True
	False
)

// keys are wrapped in struct with additional ENUM value (check comment above for more information)
type redisKeys struct {
	keys       map[string]interface{}
	resultType resultType
}

func newRedisKeys(keys map[string]interface{}, resultType resultType) *redisKeys {
	return &redisKeys{
		keys:       keys,
		resultType: resultType,
	}
}

// Formula build from physical.Formula, so that it accepts formulas for redis database
// Also, getAllKeys returns all keys, that will be subject of HGetAll
type KeyFormula interface {
	getAllKeys(ctx context.Context, variables octosql.Variables) (*redisKeys, error)
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

func (f *Constant) getAllKeys(ctx context.Context, variables octosql.Variables) (*redisKeys, error) {
	if f.value {
		return newRedisKeys(make(map[string]interface{}), True), nil
	}

	return newRedisKeys(make(map[string]interface{}), False), nil
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

func (f *And) getAllKeys(ctx context.Context, variables octosql.Variables) (*redisKeys, error) {
	leftKeys, err := f.left.getAllKeys(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from left KeyFormula")
	}

	rightKeys, err := f.right.getAllKeys(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from right KeyFormula")
	}

	if leftKeys.resultType == False || rightKeys.resultType == False { // one of children is Constant(false) - return empty map and false type
		return newRedisKeys(make(map[string]interface{}), False), nil
	}

	if leftKeys.resultType == True { // if one child is Constant(true) then we return map and type of other child
		return newRedisKeys(rightKeys.keys, rightKeys.resultType), nil
	}

	if rightKeys.resultType == True {
		return newRedisKeys(leftKeys.keys, leftKeys.resultType), nil
	}

	resultKeys := createIntersection(leftKeys.keys, rightKeys.keys) // if not, then both children returned keys map - return intersection of them

	return newRedisKeys(resultKeys, DefaultKeys), nil
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

func (f *Or) getAllKeys(ctx context.Context, variables octosql.Variables) (*redisKeys, error) {
	leftKeys, err := f.left.getAllKeys(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from left KeyFormula")
	}

	rightKeys, err := f.right.getAllKeys(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from right KeyFormula")
	}

	if leftKeys.resultType == True || rightKeys.resultType == True { // if so, then we "accept everything" at this point
		return newRedisKeys(make(map[string]interface{}), True), nil // returnType is True, because Or('sth', True) can be simplified to Constant(true)
	}

	if leftKeys.resultType == False && rightKeys.resultType == False { // if so, then we "accept nothing" at this point
		return newRedisKeys(make(map[string]interface{}), False), nil // returnType is False, because Or(False, False) can be simplified to Constant(false)
	}

	resultKeys := createUnion(leftKeys.keys, rightKeys.keys)

	return newRedisKeys(resultKeys, DefaultKeys), nil
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

func (f *Equal) getAllKeys(ctx context.Context, variables octosql.Variables) (*redisKeys, error) {
	exprValue, err := f.child.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get child expression value")
	}

	switch exprValue.GetType() {
	case octosql.TypeString:
		return newRedisKeys(
			map[string]interface{}{
				exprValue.AsString(): nil,
			}, DefaultKeys), nil

	default:
		return nil, errors.Errorf("wrong expression value for a key in redis database")
	}
}

type In struct {
	child execution.Expression
}

func NewIn(child execution.Expression) *In {
	return &In{
		child: child,
	}
}

func (f *In) getAllKeys(ctx context.Context, variables octosql.Variables) (*redisKeys, error) {
	exprValue, err := f.child.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get child expression value")
	}

	switch exprValue.GetType() {
	case octosql.TypeString:
		return newRedisKeys(
			map[string]interface{}{
				exprValue.AsString(): nil,
			}, DefaultKeys), nil
	case octosql.TypeTuple:
		out := make(map[string]interface{})

		for _, element := range exprValue.AsSlice() {
			if element.GetType() != octosql.TypeString {
				return nil, errors.Errorf("wrong expression value for a key in redis database: %v", element)
			}

			out[element.AsString()] = nil
		}

		return newRedisKeys(out, DefaultKeys), nil

	default:
		return nil, errors.Errorf("wrong expression value for a key in redis database: %v", exprValue)
	}
}

func NewKeyFormula(formula physical.Formula, key, alias string, matCtx *physical.MaterializationContext) (KeyFormula, error) {
	switch formula := formula.(type) {
	case *physical.And:
		leftFormula, err := NewKeyFormula(formula.Left, key, alias, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		rightFormula, err := NewKeyFormula(formula.Right, key, alias, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from right formula")
		}

		return NewAnd(leftFormula, rightFormula), nil

	case *physical.Or:
		leftFormula, err := NewKeyFormula(formula.Left, key, alias, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		rightFormula, err := NewKeyFormula(formula.Right, key, alias, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create KeyFormula from left formula")
		}

		return NewOr(leftFormula, rightFormula), nil

	case *physical.Predicate:
		switch formula.Relation {
		case physical.Equal:
			if !isExpressionKeyAlias(formula.Left, key, alias) {
				if !isExpressionKeyAlias(formula.Right, key, alias) {
					return nil, errors.Errorf("neither of predicates expressions represents key identifier")
				}

				materializedLeft, err := formula.Left.Materialize(context.Background(), matCtx)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't materialize left expression")
				}

				return NewEqual(materializedLeft), nil
			}

			materializedRight, err := formula.Right.Materialize(context.Background(), matCtx)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't materialize right expression")
			}

			return NewEqual(materializedRight), nil

		case physical.In:
			if !isExpressionKeyAlias(formula.Left, key, alias) {
				return nil, errors.Errorf("left hand of IN pushed down to redis must be the key")
			}

			materializedRight, err := formula.Right.Materialize(context.Background(), matCtx)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't materialize right expression")
			}

			return NewIn(materializedRight), nil

		default:
			return nil, errors.Errorf("invalid relation pushed down to redis: %v", formula.Relation)
		}

	case *physical.Constant:
		return NewConstant(formula.Value), nil

	default:
		return nil, errors.Errorf("wrong formula type for redis database")
	}
}

func isExpressionKeyAlias(expression physical.Expression, key, alias string) bool {
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

func createUnion(first, second map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range first {
		out[k] = v
	}
	for k, v := range second {
		out[k] = v
	}
	return out
}

func createIntersection(first, second map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}) // if not, then both children returned keys map - return intersection of them
	for k := range first {
		if val, ok := second[k]; ok {
			out[k] = val
		}
	}
	return out
}

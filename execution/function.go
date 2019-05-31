package execution

import (
	"math"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type FunctionType func(interface{}) (interface{}, error)

type FunctionExpression struct {
	function FunctionType
	child    Expression
}

func NewFunctionExpression(fun FunctionType, child Expression) *FunctionExpression {
	return &FunctionExpression{
		function: fun,
		child:    child,
	}
}

func (fe *FunctionExpression) ExpressionValue(variables octosql.Variables) (interface{}, error) {
	childValue, err := fe.child.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get child's expression value")
	}

	functionValue, err := fe.function(childValue)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get function value")
	}

	return functionValue, nil
}

/* Functions that are registered in the functionTable are to be implemented here */

func FuncInt(x interface{}) (interface{}, error) {
	x = NormalizeType(x)
	switch x := x.(type) {
	case int:
		return x, nil
	case float64:
		return int(x), nil
	default:
		return 0, errors.New("Variable of given type can't be cast to int")
	}
}

func FuncLower(x interface{}) (interface{}, error) {
	x = NormalizeType(x)
	switch x := x.(type) {
	case string:
		return strings.ToLower(x), nil
	default:
		return "", errors.New("Variable of given type can't be lowercased")
	}
}

func FuncUpper(x interface{}) (interface{}, error) {
	x = NormalizeType(x)
	switch x := x.(type) {
	case string:
		return strings.ToUpper(x), nil
	default:
		return "", errors.New("Variable of given type can't be uppercased")
	}
}

func FuncNegative(x interface{}) (interface{}, error) {
	x = NormalizeType(x)
	switch x := x.(type) {
	case int:
		return -x, nil
	case float64:
		return -x, nil
	default:
		return 0, errors.New("Variable of given type can't be negated")
	}
}

func FuncAbs(x interface{}) (interface{}, error) {
	x = NormalizeType(x)
	switch x := x.(type) {
	case int:
		return int(math.Abs(float64(x))), nil
	case float64:
		return math.Abs(x), nil
	default:
		return 0, errors.New("Variable of given typed doesn't have absolute value")
	}
}

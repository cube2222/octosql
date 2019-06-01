package execution

import (
	"math"
	"reflect"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type FunctionType func([]interface{}) (interface{}, error)

type FunctionExpression struct {
	function  FunctionType
	arguments []Expression
}

func NewFunctionExpression(fun FunctionType, args []Expression) *FunctionExpression {
	return &FunctionExpression{
		function:  fun,
		arguments: args,
	}
}

func (fe *FunctionExpression) ExpressionValue(variables octosql.Variables) (interface{}, error) {
	values := make([]interface{}, 0)
	for i := range fe.arguments {
		value, err := fe.arguments[i].ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get argument's expression value")
		}

		values = append(values, value)
	}

	functionValue, err := fe.function(values)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get function value")
	}

	return functionValue, nil
}

/* 	Functions that are registered in the functionTable are to be implemented here.
Each should be of type func([]interface{})(interface{}, error).
*/

func FuncInt(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("int: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case int:
		return x, nil
	case float64:
		return int(x), nil
	default:
		return 0, errors.Errorf("Can't cast variable %v of type %v to int", arg, reflect.TypeOf(arg))
	}
}

func FuncLower(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("lower: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case string:
		return strings.ToLower(x), nil
	default:
		return "", errors.Errorf("Can't lowercase variable %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func FuncUpper(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("upper: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case string:
		return strings.ToUpper(x), nil
	default:
		return "", errors.Errorf("Can't uppercase variable %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func FuncNegative(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("neg: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case int:
		return -x, nil
	case float64:
		return -x, nil
	default:
		return 0, errors.Errorf("Can't negate variable %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func FuncAbs(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("abs: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case int:
		return int(math.Abs(float64(x))), nil
	case float64:
		return math.Abs(x), nil
	default:
		return 0, errors.Errorf("Can't take absolute value of variable %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func FuncCapitalize(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("int: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case string:
		return strings.Title(x), nil
	default:
		return "", errors.Errorf("Can't capitalize variable %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func FuncSqrt(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errors.Errorf("int: expected 1 argument, got %v", len(args))
	}

	arg := args[0]
	arg = NormalizeType(arg)
	switch x := arg.(type) {
	case int:
		return math.Sqrt(float64(x)), nil
	case float64:
		return math.Sqrt(x), nil
	default:
		return 0, errors.Errorf("Can't take sqrt of variable %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func FuncMax(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, errors.New("max: expected any arguments, got 0")
	}

	max := math.Inf(-1) /* negative infinity */
	for i := range args {
		arg := args[i]
		arg = NormalizeType(arg)

		switch arg := arg.(type) {
		case int:
			max = math.Max(float64(arg), max)
		case float64:
			max = math.Max(arg, max)
		default:
			return nil, errors.Errorf("Can't take maximum of variable %v of type %v", arg, reflect.TypeOf(arg))
		}
	}

	return max, nil
}

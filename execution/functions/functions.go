package functions

import (
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

/*
	All of the functions in the funcTable must appear here.
*/

func execute(fun execution.Function, args ...interface{}) (interface{}, error) {
	err := fun.Validator(args...)
	if err != nil {
		return nil, err
	}

	return fun.Logic(args...)
}

/* Single number arguments functions */

var FuncInt = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, basicType)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			return arg, nil
		case float64:
			return int(arg), nil
		case bool:
			if arg {
				return 1, nil
			}
			return 0, nil
		case string:
			number, err := strconv.Atoi(arg)
			if err != nil {
				return nil, err
			}
			return number, nil
		default:
			return nil, errors.Errorf("Type %v can't be parsed to int", reflect.TypeOf(arg))
		}
	},
}

var FuncNegate = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			return -1 * arg, nil
		case float64:
			return -1 * arg, nil
		default:
			return nil, errors.Errorf("Type %v can't be negated", reflect.TypeOf(arg))
		}
	},
}

var FuncAbs = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			return int(math.Abs(float64(arg))), nil
		case float64:
			return math.Abs(arg), nil
		default:
			return nil, errors.Errorf("Can't take absolute value of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncSqrt = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			if arg < 0 {
				return nil, errors.Errorf("Can't take square root of value %v", arg)
			}
			return math.Sqrt(float64(arg)), nil
		case float64:
			if arg < 0 {
				return nil, errors.Errorf("Can't take square root of value %v", arg)
			}
			return math.Sqrt(arg), nil
		default:
			return nil, errors.Errorf("Can't take square root of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncFloor = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			return math.Floor(float64(arg)), nil
		case float64:
			return math.Floor(arg), nil
		default:
			return nil, errors.Errorf("Can't take floor of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncCeil = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			return math.Ceil(float64(arg)), nil
		case float64:
			return math.Ceil(arg), nil
		default:
			return nil, errors.Errorf("Can't take ceiling of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncLog = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take log of value %v", arg)
			}
			return math.Log(float64(arg)), nil
		case float64:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take log of value %v", arg)
			}
			return math.Log(arg), nil
		default:
			return nil, errors.Errorf("Can't take log of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncLn = execution.Function{
	Validator: func(args ...interface{}) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		switch arg := args[0].(type) {
		case int:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take ln of value %v", arg)
			}
			return math.Log1p(float64(arg)) - 1, nil
		case float64:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take ln of value %v", arg)
			}
			return math.Log1p(arg) - 1, nil
		default:
			return nil, errors.Errorf("Can't take ln of type %v", reflect.TypeOf(arg))
		}
	},
}

/* Multiple numbers functions */
var FuncMin = execution.Function{
	Validator: func(args ...interface{}) error {
		return atLeastOneArg(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		errInts := allInts(args...)
		errFloats := allFloats(args...)

		if errInts != nil && errFloats != nil {
			return nil, errors.Errorf("Arguments should be all ints or all floats")
		}

		if errInts == nil { /* ints */
			min := math.MinInt64
			for _, arg := range args {
				min = intMin(min, arg.(int))
			}

			return min, nil
		} else { /* floats */
			min := math.Inf(-1)
			for _, arg := range args {
				min = math.Min(min, arg.(float64))
			}

			return min, nil
		}

	},
}

var FuncMax = execution.Function{
	Validator: func(args ...interface{}) error {
		return atLeastOneArg(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		errInts := allInts(args...)
		errFloats := allFloats(args...)

		if errInts != nil && errFloats != nil {
			return nil, errors.Errorf("Arguments should be all ints or all floats")
		}

		if errInts == nil { /* ints */
			max := math.MaxInt64
			for _, arg := range args {
				max = intMax(max, arg.(int))
			}

			return max, nil
		} else { /* floats */
			max := math.Inf(1)
			for _, arg := range args {
				max = math.Max(max, arg.(float64))
			}

			return max, nil
		}

	},
}

/* Other number functions */
var FuncRand = execution.Function{
	Validator: func(args ...interface{}) error {
		err := allInts(args...)
		if err != nil {
			return err
		}

		if len(args) > 2 {
			return errors.Errorf("Expected at most two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		argCount := len(args)
		if argCount == 0 {
			return rand.Float64(), nil
		} else if argCount == 1 {
			upper := float64(args[0].(int))
			return upper * rand.Float64(), nil
		} else {
			lower := float64(args[0].(int))
			upper := float64(args[1].(int))

			return lower + (upper-lower)*rand.Float64(), nil
		}
	},
}

var FuncPower = execution.Function{
	Validator: func(args ...interface{}) error {
		err := allFloats(args...)
		if err != nil {
			return err
		}

		if len(args) != 2 {
			return errors.Errorf("Expected two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		return math.Pow(args[0].(float64), args[1].(float64)), nil
	},
}

/*  Single string functions  */

var FuncLower = execution.Function{
	Validator: func(args ...interface{}) error {
		return wantString(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		return strings.ToLower(args[0].(string)), nil
	},
}

var FuncUpper = execution.Function{
	Validator: func(args ...interface{}) error {
		return wantString(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		return strings.ToUpper(args[0].(string)), nil
	},
}

var FuncCapitalize = execution.Function{
	Validator: func(args ...interface{}) error {
		return wantString(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		return strings.ToTitle(args[0].(string)), nil
	},
}

var FuncReverse = execution.Function{
	Validator: func(args ...interface{}) error {
		return wantString(args...)
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		arg := args[0].(string)

		runes := []rune(arg)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	},
}

var FuncSubstring = execution.Function{
	Validator: func(args ...interface{}) error { /* this is a complicated validator */
		if len(args) < 2 {
			return errors.Errorf("Expected at least two arguments, got %v", len(args))
		} else if len(args) > 3 {
			return errors.Errorf("Expected at most three arguments, got %v", len(args))
		}

		err := wantString(args[0]) /* the first arg MUST be a string */
		if err != nil {
			return err
		}

		/* now we might either get (number, number) or (number) */
		err = wantInt(args[1])
		if err != nil {
			return err
		}

		if len(args) == 3 {
			err = wantInt(args[2])
			if err != nil {
				return err
			}
		}

		return nil
	},
	Logic: func(args ...interface{}) (interface{}, error) {
		str := args[0].(string)
		start := args[1].(int)
		var end int

		if len(args) == 2 {
			end = len(str)
		} else {
			end = args[2].(int)
		}

		return str[start:end], nil
	},
}

/* Auxiliary functions */
func intMin(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func intMax(x, y int) int {
	if x <= y {
		return y
	}
	return x
}

package functions

import (
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

/*
	All of the functions in the funcTable must appear here.
*/

func execute(fun execution.Function, args ...octosql.Value) (octosql.Value, error) {
	err := fun.Validator(args...)
	if err != nil {
		return nil, err
	}

	return fun.Logic(args...)
}

/* Single number arguments functions */

var FuncInt = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, basicType)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			return arg, nil
		case octosql.Float:
			return octosql.MakeInt(int(arg.Float())), nil
		case octosql.Bool:
			if arg {
				return octosql.MakeInt(1), nil
			}
			return octosql.MakeInt(0), nil
		case octosql.String:
			number, err := strconv.Atoi(arg.String())
			if err != nil {
				return nil, err
			}
			return octosql.MakeInt(number), nil
		default:
			return nil, errors.Errorf("Type %v can't be parsed to int", reflect.TypeOf(arg))
		}
	},
}

var FuncFloat = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, basicType)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			return octosql.MakeFloat(float64(arg.Int())), nil
		case octosql.Float:
			return arg, nil
		case octosql.Bool:
			if arg {
				return octosql.MakeFloat(1.0), nil
			}
			return octosql.MakeFloat(0.0), nil
		case octosql.String:
			number, err := strconv.ParseFloat(arg.String(), 64)
			if err != nil {
				return nil, err
			}
			return octosql.MakeFloat(number), nil
		default:
			return nil, errors.Errorf("Type %v can't be parsed to float64", reflect.TypeOf(arg))
		}
	},
}

var FuncNegate = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			return -1 * arg, nil
		case octosql.Float:
			return -1 * arg, nil
		default:
			return nil, errors.Errorf("Type %v can't be negated", reflect.TypeOf(arg))
		}
	},
}

var FuncAbs = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			if arg < 0 {
				return -1 * arg, nil
			}
			return arg, nil
		case octosql.Float:
			if arg < 0 {
				return -1 * arg, nil
			}
			return arg, nil
		default:
			return nil, errors.Errorf("Can't take absolute value of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncSqrt = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			if arg < 0 {
				return nil, errors.Errorf("Can't take square root of value %v", arg)
			}
			return octosql.MakeFloat(math.Sqrt(float64(arg.Int()))), nil
		case octosql.Float:
			if arg < 0 {
				return nil, errors.Errorf("Can't take square root of value %v", arg)
			}
			return octosql.MakeFloat(math.Sqrt(arg.Float())), nil
		default:
			return nil, errors.Errorf("Can't take square root of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncFloor = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			return octosql.MakeFloat(math.Floor(float64(arg.Int()))), nil
		case octosql.Float:
			return octosql.MakeFloat(math.Floor(arg.Float())), nil
		default:
			return nil, errors.Errorf("Can't take floor of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncCeil = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			return octosql.MakeFloat(math.Ceil(float64(arg.Int()))), nil
		case octosql.Float:
			return octosql.MakeFloat(math.Ceil(arg.Float())), nil
		default:
			return nil, errors.Errorf("Can't take ceiling of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncLog = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take log of value %v", arg)
			}
			return octosql.MakeFloat(math.Log2(float64(arg.Int()))), nil
		case octosql.Float:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take log of value %v", arg)
			}
			return octosql.MakeFloat(math.Log2(arg.Float())), nil
		default:
			return nil, errors.Errorf("Can't take log of type %v", reflect.TypeOf(arg))
		}
	},
}

var FuncLn = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantNumber)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		switch arg := args[0].(type) {
		case octosql.Int:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take ln of value %v", arg)
			}
			return octosql.MakeFloat(math.Log1p(float64(arg.Int())) - 1), nil
		case octosql.Float:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take ln of value %v", arg)
			}
			return octosql.MakeFloat(math.Log1p(arg.Float()) - 1), nil
		default:
			return nil, errors.Errorf("Can't take ln of type %v", reflect.TypeOf(arg))
		}
	},
}

/* Multiple numbers functions */
var FuncLeast = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return atLeastOneArg(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		errInts := allInts(args...)
		errFloats := allFloats(args...)

		if errInts != nil && errFloats != nil {
			return nil, errors.Errorf("Arguments should be all ints or all floats")
		}

		if errInts == nil { /* ints */
			var min octosql.Int = math.MaxInt64
			for _, arg := range args {
				min = intMin(min, arg.(octosql.Int))
			}

			return min, nil
		} else { /* floats */
			min := math.Inf(1)
			for _, arg := range args {
				min = math.Min(min, arg.(octosql.Float).Float())
			}

			return octosql.MakeFloat(min), nil
		}

	},
}

var FuncGreatest = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return atLeastOneArg(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		errInts := allInts(args...)
		errFloats := allFloats(args...)

		if errInts != nil && errFloats != nil {
			return nil, errors.Errorf("Arguments should be all ints or all floats")
		}

		if errInts == nil { /* ints */
			var max octosql.Int = math.MinInt64
			for _, arg := range args {
				max = intMax(max, arg.(octosql.Int))
			}

			return max, nil
		} else { /* floats */
			max := math.Inf(-1)
			for _, arg := range args {
				max = math.Max(max, arg.(octosql.Float).Float())
			}

			return octosql.MakeFloat(max), nil
		}

	},
}

/* Other number functions */
var FuncRandFloat = execution.Function{
	Validator: func(args ...octosql.Value) error {
		err := allInts(args...)
		if err != nil {
			return err
		}

		if len(args) > 2 {
			return errors.Errorf("Expected at most two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		argCount := len(args)
		if argCount == 0 {
			return octosql.MakeFloat(rand.Float64()), nil
		} else if argCount == 1 {
			upper := float64(args[0].(octosql.Int).Int())
			return octosql.MakeFloat(upper * rand.Float64()), nil
		} else {
			lower := float64(args[0].(octosql.Int).Int())
			upper := float64(args[1].(octosql.Int).Int())

			return octosql.MakeFloat(lower + (upper-lower)*rand.Float64()), nil
		}
	},
}

var FuncRandInt = execution.Function{
	Validator: func(args ...octosql.Value) error {
		err := allInts(args...)
		if err != nil {
			return err
		}

		if len(args) > 2 {
			return errors.Errorf("Expected at most two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		argCount := len(args)
		if argCount == 0 {
			return octosql.MakeInt(rand.Int()), nil
		} else if argCount == 1 {
			upper := args[0].(octosql.Int)
			if upper <= 0 {
				return nil, errors.Errorf("Upper boundary for random integer must be greater than zero")
			}

			return octosql.MakeInt(rand.Intn(upper.Int())), nil
		} else {
			lower := args[0].(octosql.Int).Int()
			upper := args[1].(octosql.Int).Int()

			if upper <= lower {
				return nil, errors.Errorf("Upper bound for random integers must be greater than the lower bound")
			}

			return octosql.MakeInt(lower + rand.Intn(upper-lower)), nil
		}
	},
}

var FuncPower = execution.Function{
	Validator: func(args ...octosql.Value) error {
		err := allFloats(args...)
		if err != nil {
			return err
		}

		if len(args) != 2 {
			return errors.Errorf("Expected two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		return octosql.MakeFloat(math.Pow(args[0].(octosql.Float).Float(), args[1].(octosql.Float).Float())), nil
	},
}

/*  Single string functions  */

var FuncLower = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantString)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		return octosql.MakeString(strings.ToLower(args[0].(octosql.String).String())), nil
	},
}

var FuncUpper = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantString)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		return octosql.MakeString(strings.ToUpper(args[0].(octosql.String).String())), nil
	},
}

var FuncCapitalize = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantString)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		arg := args[0].(octosql.String)
		arg = octosql.MakeString(strings.ToLower(arg.String()))
		return octosql.MakeString(strings.Title(arg.String())), nil
	},
}

var FuncReverse = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(oneArg, wantString)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		arg := args[0].(octosql.String)

		runes := []rune(arg)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return octosql.MakeString(string(runes)), nil
	},
}

var FuncSubstring = execution.Function{
	Validator: func(args ...octosql.Value) error { /* this is a complicated validator */
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
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		str := args[0].(octosql.String)
		start := args[1].(octosql.Int)
		var end octosql.Int

		if len(args) == 2 {
			end = octosql.MakeInt(len(str))
		} else {
			end = args[2].(octosql.Int)
		}

		return str[start:end], nil
	},
}

var FuncRegexp = execution.Function{
	Validator: func(args ...octosql.Value) error {
		return combine(twoArgs, allStrings)(args...)
	},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		re, err := regexp.Compile(args[0].(octosql.String).String())
		if err != nil {
			return nil, errors.Errorf("Couldn't compile regular expression")
		}

		match := re.FindString(args[1].(octosql.String).String())
		if match == "" {
			return nil, nil
		}

		return octosql.MakeString(match), nil
	},
}

/* Auxiliary functions */
func intMin(x, y octosql.Int) octosql.Int {
	if x <= y {
		return x
	}
	return y
}

func intMax(x, y octosql.Int) octosql.Int {
	if x <= y {
		return y
	}
	return x
}

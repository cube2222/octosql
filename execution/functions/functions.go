package functions

import (
	"log"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	. "github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

/*
	All of the functions in the funcTable must appear here.
*/

func execute(fun execution.Function, args ...Value) (Value, error) {
	err := fun.Validator(args...)
	if err != nil {
		return nil, err
	}

	return fun.Logic(args...)
}

/* Single number arguments functions */

var FuncInt = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroBool()),
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
					typeOf(ZeroString()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Bool:
			if arg {
				return MakeInt(1), nil
			}
			return MakeInt(0), nil
		case Int:
			return arg, nil
		case Float:
			return MakeInt(int(arg.AsFloat())), nil
		case String:
			number, err := strconv.Atoi(arg.AsString())
			if err != nil {
				return nil, err
			}
			return MakeInt(number), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncFloat = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroBool()),
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
					typeOf(ZeroString()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			return MakeFloat(float64(arg.AsInt())), nil
		case Float:
			return arg, nil
		case Bool:
			if arg {
				return MakeFloat(1.0), nil
			}
			return MakeFloat(0.0), nil
		case String:
			number, err := strconv.ParseFloat(arg.AsString(), 64)
			if err != nil {
				return nil, err
			}
			return MakeFloat(number), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncNegate = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			return -1 * arg, nil
		case Float:
			return -1 * arg, nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncAbs = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			if arg < 0 {
				return -1 * arg, nil
			}
			return arg, nil
		case Float:
			if arg < 0 {
				return -1 * arg, nil
			}
			return arg, nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncSqrt = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			if arg < 0 {
				return nil, errors.Errorf("Can't take square root of value %v", arg)
			}
			return MakeFloat(math.Sqrt(float64(arg.AsInt()))), nil
		case Float:
			if arg < 0 {
				return nil, errors.Errorf("Can't take square root of value %v", arg)
			}
			return MakeFloat(math.Sqrt(arg.AsFloat())), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncFloor = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			return MakeFloat(math.Floor(float64(arg.AsInt()))), nil
		case Float:
			return MakeFloat(math.Floor(arg.AsFloat())), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncCeil = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			return MakeFloat(math.Ceil(float64(arg.AsInt()))), nil
		case Float:
			return MakeFloat(math.Ceil(arg.AsFloat())), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncLog = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			allArgs(
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take log of value %v", arg)
			}
			return MakeFloat(math.Log2(float64(arg.AsInt()))), nil
		case Float:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take log of value %v", arg)
			}
			return MakeFloat(math.Log2(arg.AsFloat())), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncLn = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			arg(0,
				singleOneOf(
					typeOf(ZeroInt()),
					typeOf(ZeroFloat()),
				),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take ln of value %v", arg)
			}
			return MakeFloat(math.Log1p(float64(arg.AsInt())) - 1), nil
		case Float:
			if arg <= 0 {
				return nil, errors.Errorf("Can't take ln of value %v", arg)
			}
			return MakeFloat(math.Log1p(arg.AsFloat()) - 1), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

/* Multiple numbers functions */
var FuncLeast = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			atLeastNArgs(1),
			oneOf(
				allArgs(typeOf(ZeroInt())),
				allArgs(typeOf(ZeroFloat())),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch args[0].(type) {
		case Int:
			var min Int = math.MaxInt64
			for _, arg := range args {
				min = intMin(min, arg.(Int))
			}

			return min, nil
		case Float:
			min := math.Inf(1)
			for _, arg := range args {
				min = math.Min(min, arg.(Float).AsFloat())
			}

			return MakeFloat(min), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncGreatest = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			atLeastNArgs(1),
			oneOf(
				allArgs(typeOf(ZeroInt())),
				allArgs(typeOf(ZeroFloat())),
			),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch args[0].(type) {
		case Int:
			var max Int = math.MinInt64
			for _, arg := range args {
				max = intMax(max, arg.(Int))
			}

			return max, nil
		case Float:
			max := math.Inf(-1)
			for _, arg := range args {
				max = math.Max(max, arg.(Float).AsFloat())
			}

			return MakeFloat(max), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

/* Other number functions */
var FuncRandFloat = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			atMostNArgs(2),
			allArgs(typeOf(ZeroInt())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch len(args) {
		case 0:
			return MakeFloat(rand.Float64()), nil
		case 1:
			upper := float64(args[0].(Int).AsInt())
			return MakeFloat(upper * rand.Float64()), nil
		default:
			lower := float64(args[0].(Int).AsInt())
			upper := float64(args[1].(Int).AsInt())

			return MakeFloat(lower + (upper-lower)*rand.Float64()), nil
		}
	},
}

var FuncRandInt = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			atMostNArgs(2),
			allArgs(typeOf(ZeroInt())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		switch len(args) {
		case 0:
			return MakeInt(rand.Int()), nil
		case 1:
			upper := args[0].(Int)
			if upper <= 0 {
				return nil, errors.Errorf("Upper boundary for random integer must be greater than zero")
			}

			return MakeInt(rand.Intn(upper.AsInt())), nil
		default:
			lower := args[0].(Int).AsInt()
			upper := args[1].(Int).AsInt()

			if upper <= lower {
				return nil, errors.Errorf("Upper bound for random integers must be greater than the lower bound")
			}

			return MakeInt(lower + rand.Intn(upper-lower)), nil
		}
	},
}

var FuncPower = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(2),
			allArgs(typeOf(ZeroFloat())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		return MakeFloat(math.Pow(args[0].(Float).AsFloat(), args[1].(Float).AsFloat())), nil
	},
}

/*  Single string functions  */

var FuncLower = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			arg(0, typeOf(ZeroString())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		return MakeString(strings.ToLower(args[0].(String).AsString())), nil
	},
}

var FuncUpper = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			arg(0, typeOf(ZeroString())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		return MakeString(strings.ToUpper(args[0].(String).AsString())), nil
	},
}

var FuncCapitalize = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			arg(0, typeOf(ZeroString())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		arg := args[0].(String)
		arg = MakeString(strings.ToLower(arg.AsString()))
		return MakeString(strings.Title(arg.AsString())), nil
	},
}

var FuncReverse = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(1),
			arg(0, typeOf(ZeroString())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		arg := args[0].(String)

		runes := []rune(arg)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return MakeString(string(runes)), nil
	},
}

var FuncSubstring = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			atLeastNArgs(2),
			atMostNArgs(3),
			arg(0, typeOf(ZeroString())),
			arg(1, typeOf(ZeroInt())),
			ifArgPresent(2, arg(2, typeOf(ZeroInt()))),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		str := args[0].(String)
		start := args[1].(Int)
		end := MakeInt(len(str))

		if len(args) == 3 {
			end = args[2].(Int)
		}

		return str[start:end], nil
	},
}

var FuncRegexp = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(2),
			allArgs(typeOf(ZeroString())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		re, err := regexp.Compile(args[0].(String).AsString())
		if err != nil {
			return nil, errors.Errorf("Couldn't compile regular expression")
		}

		match := re.FindString(args[1].(String).AsString())
		if match == "" {
			return nil, nil
		}

		return MakeString(match), nil
	},
}

var FuncNth = execution.Function{
	Validator: func(args ...Value) error {
		return all(
			exactlyNArgs(2),
			arg(0, typeOf(ZeroInt())),
			arg(1, typeOf(ZeroTuple())),
		)(args...)
	},
	Logic: func(args ...Value) (Value, error) {
		return args[1].(Tuple).AsSlice()[args[0].(Int).AsInt()], nil
	},
}

/* Auxiliary functions */
func intMin(x, y Int) Int {
	if x <= y {
		return x
	}
	return y
}

func intMax(x, y Int) Int {
	if x <= y {
		return y
	}
	return x
}

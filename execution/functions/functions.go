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
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

func execute(fun execution.Function, args ...Value) (Value, error) {
	err := fun.Validator.Validate(args...)
	if err != nil {
		return nil, err
	}

	return fun.Logic(args...)
}

/* Single number arguments functions */

var FuncInt = execution.Function{
	Name: "int",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Converts x to an Integer."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroBool()),
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
				TypeOf(ZeroString()),
			),
		),
	),
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
	Name: "float",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Converts x to a Float."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroBool()),
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
				TypeOf(ZeroString()),
			),
		),
	),
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
	Name: "negate",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns x multiplied by -1."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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
	Name: "abs",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns the absolute value of x."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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
	Name: "sqrt",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns the square root of x."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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
	Name: "floor",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns the floor of x."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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
	Name: "ceil",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns the ceiling of x."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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

var FuncLog2 = execution.Function{
	Name: "log2",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns the logarithm base 2 of x."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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
	Name: "ln",
	ArgumentNames: [][]string{
		{"x"},
	},
	Description: docs.Text("Returns the natural logarithm of x."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroInt()),
				TypeOf(ZeroFloat()),
			),
		),
	),
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
	Name: "least",
	ArgumentNames: [][]string{
		{"...xs"},
	},
	Description: docs.Text("Returns the least argument."),
	Validator: All(
		AtLeastNArgs(1),
		OneOf(
			AllArgs(TypeOf(ZeroInt())),
			AllArgs(TypeOf(ZeroFloat())),
		),
	),
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
	Name: "greatest",
	ArgumentNames: [][]string{
		{"...xs"},
	},
	Description: docs.Text("Returns the greatest argument."),
	Validator: All(
		AtLeastNArgs(1),
		OneOf(
			AllArgs(TypeOf(ZeroInt())),
			AllArgs(TypeOf(ZeroFloat())),
		),
	),
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
	Name: "randfloat",
	ArgumentNames: [][]string{
		{},
		{"max"},
		{"min", "max"},
	},
	Description: docs.List(
		docs.Text("Provided no arguments, returns a pseudo-random Float in [0.0,1.0)."),
		docs.Text("Provided one argument, returns a pseudo-random Float in [0.0,max)."),
		docs.Text("Provided two arguments, returns a pseudo-random Float in [min,max)."),
	),
	Validator: All(
		AtMostNArgs(2),
		AllArgs(TypeOf(ZeroInt())),
	),
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
	Name: "randint",
	ArgumentNames: [][]string{
		{},
		{"max"},
		{"min", "max"},
	},
	Description: docs.List(
		docs.Text("Provided no arguments, returns a non-negative pseudo-random Int."),
		docs.Text("Provided one argument, returns a non-negative pseudo-random Int in [0,max)."),
		docs.Text("Provided two arguments, returns a pseudo-random Int in [min,max)."),
	),
	Validator: All(
		AtMostNArgs(2),
		AllArgs(TypeOf(ZeroInt())),
	),
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
	Name: "power",
	ArgumentNames: [][]string{
		{"x", "exponent"},
	},
	Description: docs.Text("Returns x to the power of the exponent."),
	Validator: All(
		ExactlyNArgs(2),
		AllArgs(TypeOf(ZeroFloat())),
	),
	Logic: func(args ...Value) (Value, error) {
		return MakeFloat(math.Pow(args[0].(Float).AsFloat(), args[1].(Float).AsFloat())), nil
	},
}

/*  Single string functions  */

var FuncLower = execution.Function{
	Name: "lowercase",
	ArgumentNames: [][]string{
		{"text"},
	},
	Description: docs.Text("Returns the text in lowercase."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0, TypeOf(ZeroString())),
	),
	Logic: func(args ...Value) (Value, error) {
		return MakeString(strings.ToLower(args[0].(String).AsString())), nil
	},
}

var FuncUpper = execution.Function{
	Name: "uppercase",
	ArgumentNames: [][]string{
		{"text"},
	},
	Description: docs.Text("Returns the text in uppercase."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0, TypeOf(ZeroString())),
	),
	Logic: func(args ...Value) (Value, error) {
		return MakeString(strings.ToUpper(args[0].(String).AsString())), nil
	},
}

var FuncCapitalize = execution.Function{
	Name: "capitalize",
	ArgumentNames: [][]string{
		{"text"},
	},
	Description: docs.Text("Returns the text with all words capitalized."),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0, TypeOf(ZeroString())),
	),
	Logic: func(args ...Value) (Value, error) {
		arg := args[0].(String)
		arg = MakeString(strings.ToLower(arg.AsString()))
		return MakeString(strings.Title(arg.AsString())), nil
	},
}

var FuncReverse = execution.Function{
	Name: "reverse",
	ArgumentNames: [][]string{
		{"list"},
	},
	Description: docs.List(
		docs.Text("Provided a String, returns the reversed string."),
		docs.Text("Provided a Tuple, returns the elements in reverse order."),
	),
	Validator: All(
		ExactlyNArgs(1),
		Arg(0,
			SingleOneOf(
				TypeOf(ZeroString()),
				TypeOf(ZeroTuple()),
			),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case String:
			out := make([]rune, len(arg))
			for i, el := range arg.AsString() {
				out[len(out)-i-1] = el
			}
			return MakeString(string(out)), nil
		case Tuple:
			out := make([]Value, len(arg))
			for i, el := range arg.AsSlice() {
				out[len(out)-i-1] = el
			}
			return MakeTuple(out), nil
		default:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			panic("unreachable")
		}
	},
}

var FuncSubstring = execution.Function{
	Name: "sub", //TODO: fix parsing so that you can name this function substring
	ArgumentNames: [][]string{
		{"word", "begin"},
		{"word", "begin", "end"},
	},
	Description: docs.List(
		docs.Text("Provided two arguments, returns word[begin:]"),
		docs.Text("Provided three arguments, returns word[begin:end]"),
	),
	Validator: All(
		AtLeastNArgs(2),
		AtMostNArgs(3),
		Arg(0, TypeOf(ZeroString())),
		Arg(1, TypeOf(ZeroInt())),
		IfArgPresent(2, Arg(2, TypeOf(ZeroInt()))),
	),
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

var FuncMatchRegexp = execution.Function{
	Name: "matchregexp",
	ArgumentNames: [][]string{
		{"regexp", "text"},
	},
	Description: docs.Text("Returns the first match of regexp in the text."),
	Validator: All(
		ExactlyNArgs(2),
		AllArgs(TypeOf(ZeroString())),
	),
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
	Name: "nth",
	ArgumentNames: [][]string{
		{"n", "tuple"},
	},
	Description: docs.Text("Returns the element with index n in the tuple."),
	Validator: All(
		ExactlyNArgs(2),
		Arg(0, TypeOf(ZeroInt())),
		Arg(1, TypeOf(ZeroTuple())),
	),
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

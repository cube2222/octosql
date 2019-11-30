package functions

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

func execute(fun execution.Function, args ...Value) (Value, error) {
	err := fun.Validator.Validate(args...)
	if err != nil {
		return ZeroValue(), err
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
		arg := args[0]
		switch arg.GetType() {
		case TypeBool:
			if arg.AsBool() {
				return MakeInt(1), nil
			}
			return MakeInt(0), nil
		case TypeInt:
			return arg, nil
		case TypeFloat:
			return MakeInt(int(arg.AsFloat())), nil
		case TypeString:
			number, err := strconv.Atoi(arg.AsString())
			if err != nil {
				return ZeroValue(), err
			}
			return MakeInt(number), nil
		case TypeNull, TypePhantom, TypeTime, TypeDuration, TypeTuple, TypeObject:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(arg).String())
		}
		panic("unreachable")
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
		arg := args[0]
		switch arg.GetType() {
		case TypeInt:
			return MakeFloat(float64(arg.AsInt())), nil
		case TypeFloat:
			return arg, nil
		case TypeBool:
			if arg.AsBool() {
				return MakeFloat(1.0), nil
			}
			return MakeFloat(0.0), nil
		case TypeString:
			number, err := strconv.ParseFloat(arg.AsString(), 64)
			if err != nil {
				return ZeroValue(), err
			}
			return MakeFloat(number), nil
		case TypeNull, TypePhantom, TypeTime, TypeDuration, TypeTuple, TypeObject:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(arg).String())
		}
		panic("unreachable")
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
		arg := args[0]
		switch arg.GetType() {
		case TypeInt:
			return MakeInt(-1 * arg.AsInt()), nil
		case TypeFloat:
			return MakeFloat(-1.0 * arg.AsFloat()), nil
		case TypeNull, TypePhantom, TypeBool, TypeString, TypeTime, TypeDuration, TypeTuple, TypeObject:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(arg).String())
		}
		panic("unreachable")
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
		arg := args[0]
		switch arg.GetType() {
		case TypeInt:
			asInt := arg.AsInt()
			if asInt < 0 {
				return MakeInt(-1 * asInt), nil
			}
			return arg, nil
		case TypeFloat:
			asFloat := arg.AsFloat()
			if asFloat < 0.0 {
				return MakeFloat(-1.0 * asFloat), nil
			}
			return arg, nil
		case TypeNull, TypePhantom, TypeBool, TypeString, TypeTime, TypeDuration, TypeTuple, TypeObject:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(arg).String())
		}
		panic("unreachable")
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
		arg := args[0]
		switch arg.GetType() {
		case TypeInt:
			asInt := arg.AsInt()
			if asInt < 0 {
				return ZeroValue(), fmt.Errorf("can't take square root of value %v", arg)
			}
			return MakeFloat(math.Sqrt(float64(asInt))), nil
		case TypeFloat:
			asFloat := arg.AsFloat()
			if asFloat < 0 {
				return ZeroValue(), fmt.Errorf("can't take square root of value %v", arg)
			}
			return MakeFloat(math.Sqrt(asFloat)), nil
		case TypeNull, TypePhantom, TypeBool, TypeString, TypeTime, TypeDuration, TypeTuple, TypeObject:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(arg).String())
		}
		panic("unreachable")
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
			return MakeFloat(float64(arg.AsInt())), nil
		case Float:
			return MakeFloat(math.Floor(arg.AsFloat())), nil
		case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
			return MakeFloat(float64(arg.AsInt())), nil
		case Float:
			return MakeFloat(math.Ceil(arg.AsFloat())), nil
		case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
				return nil, fmt.Errorf("can't take log of value %v", arg)
			}
			return MakeFloat(math.Log2(float64(arg.AsInt()))), nil
		case Float:
			if arg <= 0 {
				return nil, fmt.Errorf("can't take log of value %v", arg)
			}
			return MakeFloat(math.Log2(arg.AsFloat())), nil
		case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
				return nil, fmt.Errorf("can't take ln of value %v", arg)
			}
			return MakeFloat(math.Log1p(float64(arg.AsInt())) - 1), nil
		case Float:
			if arg <= 0 {
				return nil, fmt.Errorf("can't take ln of value %v", arg)
			}
			return MakeFloat(math.Log1p(arg.AsFloat()) - 1), nil
		case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
		case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
		case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
				return nil, fmt.Errorf("Upper boundary for random integer must be greater than zero")
			}

			return MakeInt(rand.Intn(upper.AsInt())), nil
		default:
			lower := args[0].(Int).AsInt()
			upper := args[1].(Int).AsInt()

			if upper <= lower {
				return nil, fmt.Errorf("Upper bound for random integers must be greater than the lower bound")
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
		case Null, Phantom, Int, Float, Bool, Time, Duration, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
	},
}

var FuncSubstring = execution.Function{
	Name: "sub", //TODO: fix parsing so that you can name this function substring
	ArgumentNames: [][]string{
		{"word", "begin"},
		{"word", "begin", "end"},
	},
	Description: docs.List(
		docs.Text("Provided two arguments, returns word[begin:]."),
		docs.Text("Provided three arguments, returns word[begin:end]."),
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
			return nil, fmt.Errorf("couldn't compile regular expression")
		}

		match := re.FindString(args[1].(String).AsString())
		if match == "" {
			return nil, nil
		}

		return MakeString(match), nil
	},
}

var FuncReplace = execution.Function{
	Name: "replace",
	ArgumentNames: [][]string{
		{"old", "new", "text"},
	},
	Description: docs.Text("Returns the text with all the instances of old replaced with new."),
	Validator: All(
		ExactlyNArgs(3),
		AllArgs(TypeOf(ZeroString())),
	),
	Logic: func(args ...Value) (Value, error) {
		return MakeString(
			strings.Replace(
				args[2].(String).AsString(),
				args[0].(String).AsString(),
				args[1].(String).AsString(),
				-1),
		), nil
	},
}

var FuncHasPrefix = execution.Function{
	Name: "hasprefix",
	ArgumentNames: [][]string{
		{"prefix", "text"},
	},
	Description: docs.Text("Returns whether the text begins with prefix."),
	Validator: All(
		ExactlyNArgs(2),
		AllArgs(TypeOf(ZeroString())),
	),
	Logic: func(args ...Value) (Value, error) {
		return MakeBool(
			strings.HasPrefix(
				args[1].(String).AsString(),
				args[0].(String).AsString(),
			),
		), nil
	},
}

var FuncHasSuffix = execution.Function{
	Name: "hassuffix",
	ArgumentNames: [][]string{
		{"suffix", "text"},
	},
	Description: docs.Text("Returns whether the text ends with suffix."),
	Validator: All(
		ExactlyNArgs(2),
		AllArgs(TypeOf(ZeroString())),
	),
	Logic: func(args ...Value) (Value, error) {
		return MakeBool(
			strings.HasSuffix(
				args[1].(String).AsString(),
				args[0].(String).AsString(),
			),
		), nil
	},
}

var FuncContains = execution.Function{
	Name: "contains",
	ArgumentNames: [][]string{
		{"substring", "text"},
		{"element", "tuple"},
	},
	Description: docs.List(
		docs.Text("Provided a String, returns whether it contains the given substring."),
		docs.Text("Provided a Tuple, returns whether it contains the given element. "),
	),
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(ZeroString())),
			Arg(1, TypeOf(ZeroTuple())),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[1].(type) {
		case String:
			return MakeBool(
				strings.Contains(
					args[1].(String).AsString(),
					args[0].(String).AsString(),
				),
			), nil
		case Tuple:
			for _, el := range arg.AsSlice() {
				if AreEqual(el, args[0]) {
					return MakeBool(true), nil
				}
			}
			return MakeBool(false), nil
		case Null, Phantom, Int, Float, Bool, Time, Duration, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
	},
}

var FuncIndex = execution.Function{
	Name: "index",
	ArgumentNames: [][]string{
		{"substring", "text"},
		{"element", "tuple"},
	},
	Description: docs.List(
		docs.Text("Provided a String, returns the index of the substring in the text, or -1 if the text doesn't contain it."),
		docs.Text("Provided a Tuple, returns the index of the element in the tuple, or -1 if the tuple doesn't contain it."),
	),
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(ZeroString())),
			Arg(1, TypeOf(ZeroTuple())),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[1].(type) {
		case String:
			return MakeInt(
				strings.Index(
					args[1].(String).AsString(),
					args[0].(String).AsString(),
				),
			), nil
		case Tuple:
			for i, el := range arg.AsSlice() {
				if AreEqual(el, args[0]) {
					return MakeInt(i), nil
				}
			}
			return MakeInt(-1), nil
		case Null, Phantom, Int, Float, Bool, Time, Duration, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
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
		if args[0].(Int).AsInt() > len(args[1].(Tuple).AsSlice()) {
			return nil, fmt.Errorf(
				"tried to access element with index %v in tuple with length %v",
				args[0].(Int).AsInt(),
				len(args[1].(Tuple).AsSlice()),
			)
		}
		return args[1].(Tuple).AsSlice()[args[0].(Int).AsInt()], nil
	},
}

var FuncLength = execution.Function{
	Name: "length",
	ArgumentNames: [][]string{
		{"seq"},
	},
	Description: docs.Text("Returns the length of the given Tuple or String."),
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
			return MakeInt(len(arg)), nil
		case Tuple:
			return MakeInt(len(arg)), nil
		case Null, Phantom, Int, Float, Bool, Time, Duration, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
	},
}

var FuncNow = execution.Function{
	Name: "now",
	ArgumentNames: [][]string{
		{""},
	},
	Description: docs.Text("Returns the current time."),
	Validator:   ExactlyNArgs(0),
	Logic: func(args ...Value) (Value, error) {
		return MakeTime(time.Now()), nil
	},
}

var FuncStringJoin = execution.Function{
	Name: "strjoin",
	ArgumentNames: [][]string{
		{"delimiter", "tuple"},
	},
	Description: docs.Text("Returns the elements of the string tuple joined into a string separated by the delimiter."),
	Validator: All(
		ExactlyNArgs(2),
		Arg(0, TypeOf(ZeroString())),
		Arg(1, TypeOf(ZeroTuple())),
	),
	Logic: func(args ...Value) (Value, error) {
		tup := args[1].(Tuple)
		out := make([]string, len(tup))
		for i := range tup {
			str, ok := tup[i].(String)
			if !ok {
				return nil, fmt.Errorf("tuple element with index %v not string, got %v", i, tup[i])
			}
			out[i] = str.AsString()
		}
		return MakeString(strings.Join(out, args[0].(String).AsString())), nil
	},
}

/* Operators */

var FuncAdd = execution.Function{
	Name: "+",
	ArgumentNames: [][]string{
		{"left", "right"},
	},
	Description: docs.Text("Returns the sum of the two arguments."),
	Validator: OneOf(
		All(
			AtLeastNArgs(1),
			AtMostNArgs(2),
			OneOf(
				AllArgs(TypeOf(ZeroInt())),
				AllArgs(TypeOf(ZeroFloat())),
				AllArgs(TypeOf(ZeroDuration())),
			),
		),
		All(
			ExactlyNArgs(2),
			Arg(0, TypeOf(ZeroTime())),
			Arg(1, TypeOf(ZeroDuration())),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch len(args) {
		case 1:
			switch args[0].(type) {
			case Int, Float, Duration:
				return args[0], nil
			case Null, Phantom, Bool, String, Time, Tuple, Object:
				log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			}
			panic("unreachable")

		case 2:
			switch arg := args[0].(type) {
			case Int:
				return arg + args[1].(Int), nil
			case Float:
				return arg + args[1].(Float), nil
			case Duration:
				return arg + args[1].(Duration), nil
			case Time:
				return MakeTime(arg.AsTime().Add(args[1].(Duration).AsDuration())), nil
			case Null, Phantom, Bool, String, Tuple, Object:
				log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			}
			panic("unreachable")

		default:
			log.Fatalf("unexpected argument count in function: %v", len(args))
			panic("unreachable")
		}
	},
}

var FuncSubtract = execution.Function{
	Name: "-",
	ArgumentNames: [][]string{
		{"left", "right"},
	},
	Description: docs.Text("Returns the difference between the two arguments."),
	Validator: OneOf(
		All(
			AtLeastNArgs(1),
			AtMostNArgs(2),
			OneOf(
				AllArgs(TypeOf(ZeroInt())),
				AllArgs(TypeOf(ZeroFloat())),
				AllArgs(TypeOf(ZeroDuration())),
			),
		),
		All(
			ExactlyNArgs(2),
			Arg(0, TypeOf(ZeroTime())),
			Arg(1, TypeOf(ZeroDuration())),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch len(args) {
		case 1:
			switch arg := args[0].(type) {
			case Int:
				return arg * -1, nil
			case Float:
				return arg * -1, nil
			case Duration:
				return arg * -1, nil
			case Null, Phantom, Bool, String, Time, Tuple, Object:
				log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			}
			panic("unreachable")

		case 2:
			switch arg := args[0].(type) {
			case Int:
				return arg - args[1].(Int), nil
			case Float:
				return arg - args[1].(Float), nil
			case Duration:
				return arg - args[1].(Duration), nil
			case Time:
				return MakeTime(arg.AsTime().Add(-1 * args[1].(Duration).AsDuration())), nil
			case Null, Phantom, Bool, String, Tuple, Object:
				log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
			}
			panic("unreachable")

		default:
			log.Fatalf("unexpected argument count in function: %v", len(args))
			panic("unreachable")
		}
	},
}

var FuncMultiply = execution.Function{
	Name: "*",
	ArgumentNames: [][]string{
		{"left", "right"},
	},
	Description: docs.Text("Returns the dot product of the two arguments."),
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(ZeroInt())),
			AllArgs(TypeOf(ZeroFloat())),
			All(
				Arg(0, TypeOf(ZeroDuration())),
				Arg(1, TypeOf(ZeroInt())),
			),
			All(
				Arg(0, TypeOf(ZeroDuration())),
				Arg(1, TypeOf(ZeroFloat())),
			),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			return arg * args[1].(Int), nil
		case Float:
			return arg * args[1].(Float), nil
		case Duration:
			switch arg2 := args[1].(type) {
			case Int:
				return MakeDuration(arg.AsDuration() * time.Duration(arg2.AsInt())), nil
			case Float:
				return MakeDuration(time.Duration(float64(arg.AsDuration()) * arg2.AsFloat())), nil
			case Null, Phantom, Bool, String, Time, Duration, Tuple, Object:
				log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[1]).String())
			}
			panic("unreachable")
		case Null, Phantom, Bool, String, Time, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
	},
}

var FuncDivide = execution.Function{
	Name: "/",
	ArgumentNames: [][]string{
		{"left", "right"},
	},
	Description: docs.Text("Returns the division of the two arguments."),
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(ZeroInt())),
			AllArgs(TypeOf(ZeroFloat())),
			All(
				Arg(0, TypeOf(ZeroDuration())),
				Arg(1, TypeOf(ZeroInt())),
			),
			All(
				Arg(0, TypeOf(ZeroDuration())),
				Arg(1, TypeOf(ZeroFloat())),
			),
			All(
				Arg(0, TypeOf(ZeroDuration())),
				Arg(1, TypeOf(ZeroDuration())),
			),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch arg := args[0].(type) {
		case Int:
			if args[1].(Int) == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return arg / args[1].(Int), nil
		case Float:
			return arg / args[1].(Float), nil
		case Duration:
			switch arg2 := args[1].(type) {
			case Int:
				if arg2 == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return MakeDuration(arg.AsDuration() / time.Duration(arg2.AsInt())), nil

			case Float:
				return MakeDuration(arg.AsDuration() / time.Duration(arg2.AsFloat())), nil

			case Duration:
				return MakeFloat(float64(arg.AsDuration()) / float64(arg2.AsDuration())), nil

			case Null, Phantom, Bool, String, Time, Tuple, Object:
				log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[1]).String())
			}
			panic("unreachable")

		case Null, Phantom, Bool, String, Time, Tuple, Object:
			log.Fatalf("unexpected type in function: %v", reflect.TypeOf(args[0]).String())
		}
		panic("unreachable")
	},
}

var FuncDuration = execution.Function{
	Name: "duration",
	ArgumentNames: [][]string{
		{"duration"},
		{"count", "unit"},
	},
	Description: docs.List(
		docs.Text("Provided one argument, parses the duration as in https://golang.org/pkg/time/#ParseDuration"),
		docs.Text("Provided two arguments, returns the duration equal to count of unit."),
	),
	Validator: OneOf(
		All(
			ExactlyNArgs(1),
			Arg(0, TypeOf(ZeroString())),
		),
		All(
			ExactlyNArgs(2),
			Arg(0, TypeOf(ZeroInt())),
			Arg(1,
				SingleAll(
					TypeOf(ZeroString()),
					ValueOf(
						MakeString("nanosecond"),
						MakeString("microsecond"),
						MakeString("millisecond"),
						MakeString("second"),
						MakeString("minute"),
						MakeString("hour"),
						MakeString("day"),
						MakeString("month"),
					),
				),
			),
		),
	),
	Logic: func(args ...Value) (Value, error) {
		switch len(args) {
		case 1:
			dur, err := time.ParseDuration(args[0].(String).AsString())
			if err != nil {
				return nil, errors.Wrap(err, "couldn't parse duration")
			}
			return MakeDuration(dur), nil

		case 2:
			count := time.Duration(args[0].(Int))
			switch args[1].(String) {
			case "nanosecond":
				return MakeDuration(count), nil
			case "microsecond":
				return MakeDuration(count * time.Microsecond), nil
			case "millisecond":
				return MakeDuration(count * time.Millisecond), nil
			case "second":
				return MakeDuration(count * time.Second), nil
			case "minute":
				return MakeDuration(count * time.Minute), nil
			case "hour":
				return MakeDuration(count * time.Hour), nil
			case "day":
				return MakeDuration(count * time.Hour * 24), nil
			default:
				log.Fatalf("unexpected value in function: %v", args[1])
				panic("unreachable")
			}

		default:
			log.Fatalf("unexpected argument count in function: %v", len(args))
			panic("unreachable")
		}
	},
}

var FuncCoalesce = execution.Function{
	Name: "coalesce",
	ArgumentNames: [][]string{
		{"...args"},
	},
	Description: docs.List(
		docs.Text("Returns the first non-null argument, or null if there isn't any."),
	),
	Validator: All(
		AtLeastNArgs(1),
	),
	Logic: func(args ...Value) (Value, error) {
		for i := range args {
			switch arg := args[i].(type) {
			case Null:
				continue
			case Phantom, Int, Float, Bool, String, Time, Duration, Tuple, Object:
				return arg, nil
			}
			panic("unreachable")
		}

		return MakeNull(), nil
	},
}

var FuncNullIf = execution.Function{
	Name: "nullif",
	ArgumentNames: [][]string{
		{"to_replace", "target"},
	},
	Description: docs.List(
		docs.Text("Returns null if target equals to_replace, returns target otherwise."),
	),
	Validator: All(
		ExactlyNArgs(2),
	),
	Logic: func(args ...Value) (Value, error) {
		if AreEqual(args[0], args[1]) {
			return MakeNull(), nil
		}

		return args[1], nil
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

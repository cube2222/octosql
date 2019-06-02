package functions

import (
	"math"
	"math/rand"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

/*
	All of the functions in the funcTable must appear here.
*/

/* Single number arguments functions */

var FuncInt = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneNumber(args)
	},
	Logic: func(args ...interface{}) interface{} {
		return args[0].(int)
	},
}

var FuncNegate = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneNumber(args)
	},
	Logic: func(args ...interface{}) interface{} {
		return args[0].(float64) * -1
	},
}

var FuncAbs = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneNumber(args)
	},
	Logic: func(args ...interface{}) interface{} {
		return math.Abs(args[0].(float64))
	},
}

var FuncSqrt = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneNonNegativeNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		arg, _ := floatify(args[0])
		return math.Sqrt(arg)
	},
}

var FuncFloor = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		arg, _ := floatify(args[0])
		return math.Floor(arg)
	},
}

var FuncCeil = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		arg, _ := floatify(args[0])
		return math.Ceil(arg)
	},
}

var FuncLog = execution.Function{
	Validator: func(args ...interface{}) error {
		return onePositiveNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		arg, _ := floatify(args[0])
		return math.Log2(arg)
	},
}

var FuncLn = execution.Function{
	Validator: func(args ...interface{}) error {
		return onePositiveNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		arg, _ := floatify(args[0])
		return math.Log1p(arg) - 1
	},
}

/* Multiple numbers functions */
var FuncMin = execution.Function{
	Validator: func(args ...interface{}) error {
		return atLeastOneNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		min := math.Inf(-1)

		for _, arg := range args {
			fArg, _ := floatify(arg)
			min = math.Min(min, fArg)
		}

		return min
	},
}

var FuncMax = execution.Function{
	Validator: func(args ...interface{}) error {
		return atLeastOneNumber(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		max := math.Inf(1)

		for _, arg := range args {
			fArg, _ := floatify(arg)
			max = math.Max(max, fArg)
		}

		return max
	},
}

/* Other number functions */
var FuncRand = execution.Function{
	Validator: func(args ...interface{}) error {
		err := allNumbers(args...)
		if err != nil {
			return err
		}

		if len(args) > 2 {
			return errors.Errorf("Expected at most two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...interface{}) interface{} {
		argCount := len(args)
		if argCount == 0 {
			return rand.Float64()
		} else if argCount == 1 {
			upper, _ := floatify(args[0])
			return upper * rand.Float64()
		} else {
			lower, _ := floatify(args[0])
			upper, _ := floatify(args[1])

			return lower + (upper-lower)*rand.Float64()
		}
	},
}

var FuncPower = execution.Function{
	Validator: func(args ...interface{}) error {
		err := allNumbers(args...)
		if err != nil {
			return err
		}

		if len(args) != 2 {
			return errors.Errorf("Expected two arguments, got %v", len(args))
		}
		return nil
	},
	Logic: func(args ...interface{}) interface{} {
		arg1, _ := floatify(args[0])
		arg2, _ := floatify(args[1])
		return math.Pow(arg1, arg2)
	},
}

/*  Single string functions  */

var FuncLower = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneString(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		return strings.ToLower(args[0].(string))
	},
}

var FuncUpper = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneString(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		return strings.ToUpper(args[0].(string))
	},
}

var FuncCapitalize = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneString(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		return strings.ToTitle(args[0].(string))
	},
}

var FuncReverse = execution.Function{
	Validator: func(args ...interface{}) error {
		return oneString(args...)
	},
	Logic: func(args ...interface{}) interface{} {
		arg := args[0].(string)

		runes := []rune(arg)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)
	},
}

var FuncSubstring = execution.Function{
	Validator: func(args ...interface{}) error { /* this is a complicated validator */
		if len(args) < 2 {
			return errors.Errorf("Expected at least two arguments, got %v", len(args))
		} else if len(args) > 3 {
			return errors.Errorf("Expected at most three arguments, got %v", len(args))
		}

		err := oneString(args[0]) /* the first arg MUST be a string */
		if err != nil {
			return err
		}

		/* now we might either get (number, number) or (number) */
		err = oneNonNegativeNumber(args[1])
		if err != nil {
			return err
		}

		if len(args) == 3 {
			err = oneNonNegativeNumber(args[2])
			if err != nil {
				return err
			}
		}

		return nil
	},
	Logic: func(args ...interface{}) interface{} {
		str := args[0].(string)
		fStart, _ := floatify(args[1])
		start := int(fStart)

		var end int

		if len(args) == 2 {
			end = len(str)
		} else {
			fEnd, _ := floatify(args[2])
			end = int(fEnd)
		}

		return str[start:end]
	},
}

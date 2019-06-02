package functions

import (
	"reflect"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

func oneNumber(args ...interface{}) error {
	if len(args) != 1 {
		return errors.Errorf("Expected one argument, got %v", len(args))
	}

	arg := execution.NormalizeType(args[0])
	argType := reflect.TypeOf(arg)

	if argType != reflect.TypeOf(0) && argType != reflect.TypeOf(0.0) {
		return errors.Errorf("Expected numeric argument, got %v of type %v", arg, argType)
	}

	return nil
}

func allNumbers(args ...interface{}) error {
	for i := range args {
		err := oneNumber(args[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func oneString(args ...interface{}) error {
	if len(args) != 1 {
		return errors.Errorf("Expected one argument, got %v", len(args))
	}

	arg := execution.NormalizeType(args[0])
	argType := reflect.TypeOf(arg)

	if argType != reflect.TypeOf("") {
		return errors.Errorf("Expected string argument, got %v of type %v", arg, argType)
	}

	return nil
}

func allStrings(args ...interface{}) error {
	for i := range args {
		err := oneString(args[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func atLeastOneOf(typeValidator func(...interface{}) error, args ...interface{}) error {
	err := typeValidator(args...)
	if err != nil {
		return err
	}

	if len(args) == 0 {
		return errors.Errorf("Expected at least one argument, got none")
	}

	return nil
}

func atLeastOneNumber(args ...interface{}) error {
	return atLeastOneOf(oneString, args...)
}

func atLeastOneString(args ...interface{}) error {
	return atLeastOneOf(oneNumber, args...)
}

func oneNonNegativeNumber(args ...interface{}) error {
	err := oneNumber(args...)
	if err != nil {
		return err
	}

	arg, _ := floatify(args[0]) /* ignore err since we validate */

	if arg < 0 {
		return errors.Errorf("Argument should be non-negative, got %v", arg)
	}

	return nil
}

func onePositiveNumber(args ...interface{}) error {
	err := oneNumber(args...)
	if err != nil {
		return err
	}

	arg, _ := floatify(args[0]) /* ignore err since we validate */
	if arg <= 0 {
		return errors.Errorf("Argument should be positive, got %v", arg)
	}

	return nil
}

func floatify(x interface{}) (float64, error) {
	x = execution.NormalizeType(x)
	switch x := x.(type) {
	case int:
		return float64(x), nil
	case float64:
		return x, nil
	default:
		return 0.0, errors.Errorf("Value %v of type %v can't be cast to float",
			x, reflect.TypeOf(x))
	}
}

package functions

import (
	"reflect"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

func combine(validators ...func(...octosql.Value) error) func(...octosql.Value) error {
	return func(args ...octosql.Value) error {
		for _, validator := range validators {
			err := validator(args...)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func exactlyNArgs(n int, args ...octosql.Value) error {
	if len(args) != n {
		return errors.Errorf("Expected exactly %v arguments, but got %v", n, len(args))
	}
	return nil
}

func oneArg(args ...octosql.Value) error { /* this is a popular validator */
	return exactlyNArgs(1, args...)
}

func twoArgs(args ...octosql.Value) error {
	return exactlyNArgs(2, args...)
}

func atLeastOneArg(args ...octosql.Value) error {
	if len(args) == 0 {
		return errors.Errorf("Expected at least one argument, but got zero")
	}
	return nil
}

func basicType(args ...octosql.Value) error {
	switch arg := args[0].(type) {
	case octosql.Int:
		return nil
	case octosql.Float:
		return nil
	case octosql.Bool:
		return nil
	case octosql.String:
		return nil
	default:
		return errors.Errorf("Expected a basic type, but got %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func wantInt(args ...octosql.Value) error {
	_, ok := args[0].(octosql.Int)
	if !ok {
		return errors.Errorf("Expected an int, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
	}
	return nil
}

func wantFloat(args ...octosql.Value) error {
	_, ok := args[0].(octosql.Float)
	if !ok {
		return errors.Errorf("Expected a float, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
	}
	return nil
}

func wantString(args ...octosql.Value) error {
	_, ok := args[0].(octosql.String)
	if !ok {
		return errors.Errorf("Expected a string, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
	}
	return nil
}

func wantNumber(args ...octosql.Value) error {
	_, ok := args[0].(octosql.Int)
	if !ok {
		_, ok := args[0].(octosql.Float)
		if !ok {
			return errors.Errorf("Expected a number, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
		}
	}

	return nil
}

func allInts(args ...octosql.Value) error {
	for _, arg := range args {
		err := wantInt(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

func allFloats(args ...octosql.Value) error {
	for _, arg := range args {
		err := wantFloat(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

func allNumbers(args ...octosql.Value) error {
	for _, arg := range args {
		err := wantNumber(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

func allStrings(args ...octosql.Value) error {
	for _, arg := range args {
		err := wantString(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

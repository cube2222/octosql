package functions

import (
	"reflect"

	"github.com/pkg/errors"
)

func combine(validators ...func(...interface{}) error) func(...interface{}) error {
	return func(args ...interface{}) error {
		for _, validator := range validators {
			err := validator(args...)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func exactlyNArgs(n int, args ...interface{}) error {
	if len(args) != n {
		return errors.Errorf("Expected exactly %v arguments, but got %v", n, len(args))
	}
	return nil
}

func oneArg(args ...interface{}) error { /* this is a popular validator */
	return exactlyNArgs(1, args...)
}

func twoArgs(args ...interface{}) error {
	return exactlyNArgs(2, args...)
}

func atLeastOneArg(args ...interface{}) error {
	if len(args) == 0 {
		return errors.Errorf("Expected at least one argument, but got zero")
	}
	return nil
}

func basicType(args ...interface{}) error {
	switch arg := args[0].(type) {
	case int:
		return nil
	case float64:
		return nil
	case bool:
		return nil
	case string:
		return nil
	default:
		return errors.Errorf("Expected a basic type, but got %v of type %v", arg, reflect.TypeOf(arg))
	}
}

func wantInt(args ...interface{}) error {
	_, ok := args[0].(int)
	if !ok {
		return errors.Errorf("Expected an int, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
	}
	return nil
}

func wantFloat(args ...interface{}) error {
	_, ok := args[0].(float64)
	if !ok {
		return errors.Errorf("Expected a float, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
	}
	return nil
}

func wantString(args ...interface{}) error {
	_, ok := args[0].(string)
	if !ok {
		return errors.Errorf("Expected a string, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
	}
	return nil
}

func wantNumber(args ...interface{}) error {
	_, ok := args[0].(int)
	if !ok {
		_, ok := args[0].(float64)
		if !ok {
			return errors.Errorf("Expected a number, but got %v of type %v", args[0], reflect.TypeOf(args[0]))
		}
	}

	return nil
}

func allInts(args ...interface{}) error {
	for _, arg := range args {
		err := wantInt(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

func allFloats(args ...interface{}) error {
	for _, arg := range args {
		err := wantFloat(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

func allNumbers(args ...interface{}) error {
	for _, arg := range args {
		err := wantNumber(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

func allStrings(args ...interface{}) error {
	for _, arg := range args {
		err := wantString(arg)
		if err != nil {
			return err
		}
	}

	return nil
}

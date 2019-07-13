package functions

import (
	"log"
	"reflect"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Validator func(args ...octosql.Value) error
type SingleArgumentValidator func(arg octosql.Value) error

func all(validators ...Validator) Validator {
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

func singleAll(validators ...SingleArgumentValidator) SingleArgumentValidator {
	return func(args octosql.Value) error {
		for _, validator := range validators {
			err := validator(args)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func oneOf(validators ...Validator) Validator {
	return func(args ...octosql.Value) error {
		errs := make([]error, len(validators))
		for i, validator := range validators {
			errs[i] = validator(args...)
			if errs[i] == nil {
				return nil
			}
		}

		return errors.Errorf("none of the conditions have been met: %+v", errs)
	}
}

func singleOneOf(validators ...SingleArgumentValidator) SingleArgumentValidator {
	return func(args octosql.Value) error {
		errs := make([]error, len(validators))
		for i, validator := range validators {
			errs[i] = validator(args)
			if errs[i] == nil {
				return nil
			}
		}

		return errors.Errorf("none of the conditions have been met: %+v", errs)
	}
}

func ifArgPresent(i int, validator Validator) Validator {
	return func(args ...octosql.Value) error {
		if len(args) < i+1 {
			return nil
		}
		return validator(args...)
	}
}

func atLeastNArgs(n int) Validator {
	return func(args ...octosql.Value) error {
		if len(args) < n {
			return errors.Errorf("expected at least %v arguments, but got %v", n, len(args))
		}
		return nil
	}
}

func atMostNArgs(n int) Validator {
	return func(args ...octosql.Value) error {
		if len(args) > n {
			return errors.Errorf("expected at most %v arguments, but got %v", n, len(args))
		}
		return nil
	}
}

func exactlyNArgs(n int) Validator {
	return func(args ...octosql.Value) error {
		if len(args) != n {
			return errors.Errorf("expected exactly %v arguments, but got %v", n, len(args))
		}
		return nil
	}
}

func typeOf(wantedType octosql.Value) SingleArgumentValidator {
	switch wantedType.(type) {
	case octosql.Phantom:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Phantom); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroPhantom()).String(), arg)
			}
			return nil
		}

	case octosql.Int:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Int); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroInt()).String(), arg)
			}
			return nil
		}

	case octosql.Float:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Float); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroFloat()).String(), arg)
			}
			return nil
		}

	case octosql.Bool:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Bool); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroBool()).String(), arg)
			}
			return nil
		}

	case octosql.String:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.String); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroString()).String(), arg)
			}
			return nil
		}

	case octosql.Time:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Time); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroTime()).String(), arg)
			}
			return nil
		}

	case octosql.Tuple:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Tuple); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroTuple()).String(), arg)
			}
			return nil
		}

	case octosql.Object:
		return func(arg octosql.Value) error {
			if _, ok := arg.(octosql.Object); !ok {
				return errors.Errorf("expected type %v but got %v", reflect.TypeOf(octosql.ZeroObject()).String(), arg)
			}
			return nil
		}

	}

	log.Fatalf("unhandled type: %v", reflect.TypeOf(wantedType).String())
	panic("unreachable")
}

func arg(i int, validator SingleArgumentValidator) Validator {
	return func(args ...octosql.Value) error {
		if err := validator(args[i]); err != nil {
			return errors.Errorf("bad argument at index %v: %v", i, err)
		}
		return nil
	}
}

func allArgs(validator SingleArgumentValidator) Validator {
	return func(args ...octosql.Value) error {
		for i := range args {
			if err := validator(args[i]); err != nil {
				return errors.Errorf("bad argument at index %v: %v", i, err)
			}
		}
		return nil
	}
}

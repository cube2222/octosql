package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Function struct {
	Validator func(...octosql.Value) error
	Logic     func(...octosql.Value) (octosql.Value, error)
}

type FunctionExpression struct {
	function  Function
	arguments []Expression
}

func NewFunctionExpression(fun Function, args []Expression) *FunctionExpression {
	return &FunctionExpression{
		function:  fun,
		arguments: args,
	}
}

func (fe *FunctionExpression) ExpressionValue(variables octosql.Variables) (octosql.Value, error) {
	values := make([]octosql.Value, 0)
	for i := range fe.arguments {
		value, err := fe.arguments[i].ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get argument's expression value")
		}

		values = append(values, value)
	}

	err := fe.function.Validator(values...)
	if err != nil {
		return nil, err
	}

	finalValue, err := fe.function.Logic(values...)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get function's value")
	}

	return finalValue, nil
}

package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Function struct {
	Validator func(...interface{}) error
	Logic     func(...interface{}) (interface{}, error)
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

func (fe *FunctionExpression) ExpressionValue(variables octosql.Variables) (interface{}, error) {
	values := make([]interface{}, 0)
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

	return fe.function.Logic(values...) /* return value and possible error */
}

package execution

import (
	"fmt"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/pkg/errors"
)

type Function struct {
	Name          string
	ArgumentNames [][]string
	Description   docs.Documentation
	Validator     Validator
	Logic         func(...octosql.Value) (octosql.Value, error)
}

func (f *Function) Document() docs.Documentation {
	callingWays := make([]docs.Documentation, len(f.ArgumentNames))
	for i, arguments := range f.ArgumentNames {
		callingWays[i] = docs.Text(fmt.Sprintf("%s(%s)", f.Name, strings.Join(arguments, ", ")))
	}
	return docs.Section(
		f.Name,
		docs.Body(
			docs.Section("Calling", docs.List(callingWays...)),
			docs.Section("Arguments", f.Validator.Document()),
			docs.Section("Description", f.Description),
		),
	)
}

type Validator interface {
	docs.Documented
	Validate(args ...octosql.Value) error
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

	err := fe.function.Validator.Validate(values...)
	if err != nil {
		return nil, err
	}

	finalValue, err := fe.function.Logic(values...)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get function's value")
	}

	return finalValue, nil
}

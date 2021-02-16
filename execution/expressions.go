package execution

import (
	"fmt"

	"github.com/cube2222/octosql/octosql"
)

const BTreeDefaultDegree = 12

type Expression interface {
	Evaluate(ctx ExecutionContext) (octosql.Value, error)
}

type Variable struct {
	level, index int
}

func NewVariable(level, index int) *Variable {
	return &Variable{
		level: level,
		index: index,
	}
}

func (r *Variable) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	curVars := ctx.VariableContext
	for i := r.level; i != 0; i-- {
		curVars = curVars.Parent
	}
	return curVars.Values[r.index], nil
}

type Constant struct {
	value octosql.Value
}

func NewConstant(value octosql.Value) *Constant {
	return &Constant{
		value: value,
	}
}

func (c *Constant) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	return c.value, nil
}

type TypeAssertion struct {
	expected octosql.Type
	expr     Expression
}

func NewTypeAssertion(expected octosql.Type, expr Expression) *TypeAssertion {
	return &TypeAssertion{
		expected: expected,
		expr:     expr,
	}
}

func (c *TypeAssertion) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	value, err := c.expr.Evaluate(ctx)
	if err != nil {
		return octosql.ZeroValue, err
	}

	if value.Type.Is(c.expected) != octosql.TypeRelationIs {
		return octosql.ZeroValue, fmt.Errorf("invalid type: %s, expected: %s", value.Type, c.expected)
	}

	return value, nil
}

type FunctionCall struct {
	function         func([]octosql.Value) (octosql.Value, error)
	args             []Expression
	nullCheckIndices []int
}

func NewFunctionCall(function func([]octosql.Value) (octosql.Value, error), args []Expression, nullCheckIndices []int) *FunctionCall {
	return &FunctionCall{
		function:         function,
		args:             args,
		nullCheckIndices: nullCheckIndices,
	}
}

func (c *FunctionCall) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	argValues := make([]octosql.Value, len(c.args))
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d argument: %w", i, err)
		}
		argValues[i] = value
	}
	for _, index := range c.nullCheckIndices {
		if argValues[index].Type.TypeID == octosql.TypeIDNull {
			return octosql.NewNull(), nil
		}
	}

	value, err := c.function(argValues)
	if err != nil {
		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate function: %w", err)
	}
	return value, nil
}

type And struct {
	args []Expression
}

func NewAnd(args []Expression) *And {
	return &And{
		args: args,
	}
}

func (c *And) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d argument: %w", i, err)
		}
		if !value.Boolean {
			return value, nil
		}
	}

	return octosql.NewBoolean(true), nil
}

type Or struct {
	args []Expression
}

func NewOr(args []Expression) *Or {
	return &Or{
		args: args,
	}
}

func (c *Or) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d argument: %w", i, err)
		}
		if value.Boolean {
			return value, nil
		}
	}

	return octosql.NewBoolean(false), nil
}

// TODO: sys.undo should create an expression which reads the current retraction status.

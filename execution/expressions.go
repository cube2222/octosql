package execution

import (
	"fmt"

	"github.com/cube2222/octosql"
)

const BTreeDefaultDegree = 12

type Expression interface {
	Evaluate(ctx ExecutionContext) (octosql.Value, error)
}

type Variable struct {
	level, index int
}

func (r *Variable) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	curVars := ctx.VariableContext
	for i := r.level; i != 0; i-- {
		curVars = curVars.Parent
	}
	return curVars.Values[r.index], nil
}

func NewVariable(level, index int) *Variable {
	return &Variable{
		level: level,
		index: index,
	}
}

type Constant struct {
	value octosql.Value
}

func (c *Constant) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	return c.value, nil
}

func NewConstant(value octosql.Value) *Constant {
	return &Constant{
		value: value,
	}
}

type TypeAssertion struct {
	expected octosql.Type
	expr     Expression
}

func (c *TypeAssertion) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	value, err := c.expr.Evaluate(ctx)
	if err != nil {
		return octosql.ZeroValue, err
	}

	if !value.Type.Is(c.expected) {
		return octosql.ZeroValue, fmt.Errorf("invalid type: %s, expected: %s", value.Type, c.expected)
	}

	return value, nil
}

func NewTypeAssertion(expected octosql.Type, expr Expression) *TypeAssertion {
	return &TypeAssertion{
		expected: expected,
		expr:     expr,
	}
}

type FunctionCall struct {
	function func([]octosql.Value) (octosql.Value, error)
	args     []Expression
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

	value, err := c.function(argValues)
	if err != nil {
		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate function: %w", err)
	}
	return value, nil
}

func NewFunctionCall(function func([]octosql.Value) (octosql.Value, error), args []Expression) *FunctionCall {
	return &FunctionCall{
		function: function,
		args:     args,
	}
}

type And struct {
	args []Expression
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

func NewAnd(args []Expression) *And {
	return &And{
		args: args,
	}
}

type Or struct {
	args []Expression
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

func NewOr(args []Expression) *Or {
	return &Or{
		args: args,
	}
}

// TODO: sys.undo should create an expression which reads the current retraction status.

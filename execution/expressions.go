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

	if c.expected.TypeID == octosql.TypeIDStruct {
		return octosql.ZeroValue, fmt.Errorf("type assertions for structures aren't yet supported")
	}
	if value.Type().Is(c.expected) != octosql.TypeRelationIs {
		return octosql.ZeroValue, fmt.Errorf("invalid type: %s, expected: %s", value.Type(), c.expected)
	}

	return value, nil
}

type Cast struct {
	targetType octosql.Type
	expr       Expression
}

func NewCast(targetType octosql.Type, expr Expression) *Cast {
	return &Cast{
		targetType: targetType,
		expr:       expr,
	}
}

func (c *Cast) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	value, err := c.expr.Evaluate(ctx)
	if err != nil {
		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate cast argument: %w", err)
	}

	if c.targetType.TypeID == octosql.TypeIDStruct {
		return octosql.ZeroValue, fmt.Errorf("type assertions for structures aren't yet supported")
	}
	if value.Type().Is(c.targetType) != octosql.TypeRelationIs {
		return octosql.NewNull(), nil
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
		if argValues[index].TypeID == octosql.TypeIDNull {
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
	nullEncountered := false
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d AND argument: %w", i, err)
		}
		if value.TypeID == octosql.TypeIDNull {
			nullEncountered = true
			continue
		}
		if !value.Boolean {
			return value, nil
		}
	}
	if nullEncountered {
		return octosql.NewNull(), nil
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
	nullEncountered := false
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d OR argument: %w", i, err)
		}
		if value.Boolean {
			return value, nil
		}
		if value.TypeID == octosql.TypeIDNull {
			nullEncountered = true
		}
	}
	if nullEncountered {
		return octosql.NewNull(), nil
	}
	return octosql.NewBoolean(false), nil
}

type SingleColumnQueryExpression struct {
	source Node
}

func NewSingleColumnQueryExpression(source Node) *SingleColumnQueryExpression {
	return &SingleColumnQueryExpression{
		source: source,
	}
}

func (e *SingleColumnQueryExpression) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	// TODO: Handle retractions.
	var values []octosql.Value
	e.source.Run(
		ctx,
		func(ctx ProduceContext, record Record) error {
			if record.Retraction {
				return fmt.Errorf("query expression currently can't handle retractions")
			}
			values = append(values, record.Values[0])
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error { return nil },
	)
	return octosql.NewList(values), nil
}

type MultiColumnQueryExpression struct {
	source Node
}

func NewMultiColumnQueryExpression(source Node) *MultiColumnQueryExpression {
	return &MultiColumnQueryExpression{
		source: source,
	}
}

func (e *MultiColumnQueryExpression) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	// TODO: Handle retractions.
	var values []octosql.Value
	e.source.Run(
		ctx,
		func(ctx ProduceContext, record Record) error {
			if record.Retraction {
				return fmt.Errorf("query expression currently can't handle retractions")
			}
			values = append(values, octosql.NewStruct(record.Values))
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error { return nil },
	)
	return octosql.NewList(values), nil
}

type Coalesce struct {
	args []Expression
}

func NewCoalesce(args []Expression) *Coalesce {
	return &Coalesce{
		args: args,
	}
}

func (c *Coalesce) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d OR argument: %w", i, err)
		}
		if value.TypeID != octosql.TypeIDNull {
			return value, nil
		}
	}
	return octosql.NewNull(), nil
}

type Tuple struct {
	args []Expression
}

func NewTuple(args []Expression) *Tuple {
	return &Tuple{
		args: args,
	}
}

func (c *Tuple) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	values := make([]octosql.Value, len(c.args))
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d tuple argument: %w", i, err)
		}
		values[i] = value
	}
	return octosql.NewTuple(values), nil
}

// TODO: sys.undo should create an expression which reads the current retraction status.

package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql"
)

type Node interface {
	Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error
}

type ExecutionContext struct {
	context.Context
	VariableContext *VariableContext
}

type VariableContext struct {
	Parent *VariableContext
	Values []octosql.Value
}

func (varCtx *VariableContext) WithRecord(record Record) *VariableContext {
	return &VariableContext{
		Parent: varCtx,
		Values: record.Values,
	}
}

type ProduceFn func(ctx ProduceContext, record Record) error

type ProduceContext struct {
	context.Context
}

func ProduceFromExecutionContext(ctx ExecutionContext) ProduceContext {
	return ProduceContext{
		Context: ctx.Context,
	}
}

type Record struct {
	Values []octosql.Value
}

type MetaSendFn func(ctx ProduceContext, msg MetadataMessage) error

type MetadataMessage struct {
	Type      MetadataMessageType
	Watermark time.Time
}

type MetadataMessageType int

const (
	MetadataMessageTypeWatermark MetadataMessageType = iota
)

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

func NewTypeAssert(expected octosql.Type, expr Expression) *TypeAssertion {
	return &TypeAssertion{
		expected: expected,
		expr:     expr,
	}
}

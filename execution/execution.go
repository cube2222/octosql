package execution

import (
	"context"
	"strings"
	"time"

	"github.com/cube2222/octosql/octosql"
)

type Node interface {
	Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error
}

type ExecutionContext struct {
	context.Context
	VariableContext *VariableContext
}

func (ctx ExecutionContext) WithRecord(record Record) ExecutionContext {
	return ExecutionContext{
		Context:         ctx.Context,
		VariableContext: ctx.VariableContext.WithRecord(record),
	}
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
	Values     []octosql.Value
	Retraction bool
}

// Functional options?
func NewRecord(values []octosql.Value, retraction bool) Record {
	return Record{
		Values:     values,
		Retraction: retraction,
	}
}

func (record Record) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	if !record.Retraction {
		builder.WriteString("+")
	} else {
		builder.WriteString("-")
	}
	builder.WriteString("| ")
	for i := range record.Values {
		builder.WriteString(record.Values[i].String())
		if i != len(record.Values)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(" |}")
	return builder.String()
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

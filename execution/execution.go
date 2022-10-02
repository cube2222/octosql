package execution

import (
	"context"
	"math"
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
	CurrentRecords  RecordBatch
	// TODO: Fix the subquery expression to handle this properly.
}

func (ctx ExecutionContext) WithRecord(record RecordBatch) ExecutionContext {
	return ExecutionContext{
		Context:         ctx.Context,
		VariableContext: ctx.VariableContext,
		CurrentRecords:  record,
	}
}

type VariableContext struct {
	Parent *VariableContext
	Values []octosql.Value
}

func (varCtx *VariableContext) WithValues(values []octosql.Value) *VariableContext {
	return &VariableContext{
		Parent: varCtx,
		Values: values,
	}
}

type ProduceFn func(ctx ProduceContext, records RecordBatch) error

type ProduceContext struct {
	context.Context
}

func ProduceFromExecutionContext(ctx ExecutionContext) ProduceContext {
	return ProduceContext{
		Context: ctx.Context,
	}
}

func ProduceFnApplyContext(fn ProduceFn, ctx ProduceContext) func(record RecordBatch) error {
	return func(record RecordBatch) error {
		return fn(ctx, record)
	}
}

// TODO: Tune this size.
const DesiredBatchSize = 8192

type RecordBatch struct {
	// Values are stored column-first.
	Values      [][]octosql.Value
	Retractions []bool      // TODO: If stream without retractions, then don't store.
	EventTimes  []time.Time // TODO: If stream without event times, then don't store.
	Size        int
}

func NewRecordBatch(values [][]octosql.Value, retractions []bool, eventTimes []time.Time) RecordBatch {
	return RecordBatch{
		Values:      values,
		Retractions: retractions,
		EventTimes:  eventTimes,
		Size:        len(retractions),
	}
}

func (records RecordBatch) Row(index int) []octosql.Value {
	return Row(records.Values, index)
}

// TODO: Check and force inlining.
func Row(columnFirstValues [][]octosql.Value, rowIndex int) []octosql.Value {
	out := make([]octosql.Value, len(columnFirstValues))
	for col := range columnFirstValues {
		out[col] = columnFirstValues[col][rowIndex]
	}
	return out
}

func (records RecordBatch) String() []string {
	out := make([]string, len(records.Values))
	for i := 0; i < records.Size; i++ {
		builder := strings.Builder{}
		builder.WriteString("{")
		if !records.Retractions[i] {
			builder.WriteString("+")
		} else {
			builder.WriteString("-")
		}
		builder.WriteString(records.EventTimes[i].Format(time.RFC3339))
		builder.WriteString("| ")
		for j := range records.Values {
			builder.WriteString(records.Values[j][i].String())
			if j != len(records.Values)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(" |}")
		out[i] = builder.String()
	}
	return out
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

var WatermarkMaxValue = time.Unix(0, math.MaxInt64)

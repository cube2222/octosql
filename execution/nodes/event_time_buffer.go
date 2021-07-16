package nodes

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
)

type EventTimeBuffer struct {
	source Node
}

func NewEventTimeBuffer(source Node) *EventTimeBuffer {
	return &EventTimeBuffer{source: source}
}

func (e *EventTimeBuffer) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	records := NewRecordEventTimeBuffer()

	if err := e.source.Run(
		ctx,
		func(ctx ProduceContext, record Record) error {
			records.AddRecord(record)
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			if msg.Type == MetadataMessageTypeWatermark {
				if err := records.Emit(msg.Watermark, ProduceFnApplyContext(produce, ctx)); err != nil {
					return fmt.Errorf("couldn't emit records up to watermark: %w", err)
				}
			}
			return metaSend(ctx, msg)
		}); err != nil {
		return err
	}

	if err := records.Emit(WatermarkMaxValue, ProduceFnApplyContext(produce, ProduceFromExecutionContext(ctx))); err != nil {
		return fmt.Errorf("couldn't emit remaining records: %w", err)
	}
	return nil
}

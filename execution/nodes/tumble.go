package nodes

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

// TODO: Watermark trigger

type Tumble struct {
	source         Node
	timeFieldIndex int
	windowLength   Expression
	offset         Expression
}

func NewTumble(
	source Node,
	timeFieldIndex int,
	windowLength Expression,
	offset Expression,
) *Tumble {
	return &Tumble{
		source:         source,
		timeFieldIndex: timeFieldIndex,
		windowLength:   windowLength,
		offset:         offset,
	}
}

func (t *Tumble) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	windowLength, err := t.windowLength.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate window_length: %w", err)
	}
	offset, err := t.offset.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate offset: %w", err)
	}

	if err := t.source.Run(ctx, func(ctx ProduceContext, record Record) error {
		timeValue := record.Values[t.timeFieldIndex].Time
		windowStart := timeValue.Add(-1 * offset.Duration).Truncate(windowLength.Duration).Add(offset.Duration)
		windowEnd := windowStart.Add(windowLength.Duration)
		record.Values = append(record.Values, octosql.NewTime(windowStart), octosql.NewTime(windowEnd))

		if err := produce(ctx, record); err != nil {
			return fmt.Errorf("couldn't produce record")
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	return nil
}

package nodes

import (
	"fmt"
	"time"

	"github.com/cube2222/octosql"
	. "github.com/cube2222/octosql/execution"
)

// TODO: Watermark trigger

type Tumble struct {
	source         Node
	timeFieldIndex int
	windowLength   time.Duration
	offset         time.Duration
}

func NewTumble(
	source Node,
	timeFieldIndex int,
	windowLength time.Duration,
	offset time.Duration,
) *Tumble {
	return &Tumble{
		source:         source,
		timeFieldIndex: timeFieldIndex,
		windowLength:   windowLength,
		offset:         offset,
	}
}

func (t *Tumble) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	if err := t.source.Run(ctx, func(ctx ProduceContext, record Record) error {
		timeValue := record.Values[t.timeFieldIndex].Time
		windowStart := timeValue.Add(-1 * t.offset).Truncate(t.windowLength).Add(t.offset)
		windowEnd := windowStart.Add(t.windowLength)
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

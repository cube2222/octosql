package nodes

import (
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
)

type MaxDifferenceWatermark struct {
	source         Node
	maxDifference  time.Duration
	timeFieldIndex int
}

func NewMaxDifferenceWatermark(
	source Node,
	maxDifference time.Duration,
	timeFieldIndex int,
) *MaxDifferenceWatermark {
	return &MaxDifferenceWatermark{
		source:         source,
		maxDifference:  maxDifference,
		timeFieldIndex: timeFieldIndex,
	}
}

func (m *MaxDifferenceWatermark) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	maxValue := time.Time{}

	if err := m.source.Run(ctx, func(ctx ProduceContext, record Record) error {
		if err := produce(ctx, record); err != nil {
			return fmt.Errorf("couldn't produce record")
		}

		if curTimeValue := record.Values[m.timeFieldIndex].Time; curTimeValue.After(maxValue) {
			maxValue = curTimeValue

			// TODO: Think about adding granularity here. (so i.e. only have second resolution)
			if err := metaSend(ctx, MetadataMessage{
				Type:      MetadataMessageTypeWatermark,
				Watermark: curTimeValue.Add(-m.maxDifference),
			}); err != nil {
				return fmt.Errorf("couldn't send updated watermark")
			}
		}

		return nil
	}, func(ctx ProduceContext, msg MetadataMessage) error {
		if msg.Type != MetadataMessageTypeWatermark {
			return metaSend(ctx, msg)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	return nil
}

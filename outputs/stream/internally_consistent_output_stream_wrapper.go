package stream

import (
	"fmt"
	"log"
	"time"

	. "github.com/cube2222/octosql/execution"
)

type InternallyConsistentOutputStreamWrapper struct {
	Source Node
}

func (node *InternallyConsistentOutputStreamWrapper) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	var pending []Record

	sendPendingLessOrEqualWatermark := func(ctx ProduceContext, watermark time.Time) error {
		crossedOut := make([]bool, len(pending))

		afterWatermarkCount := 0
		for i := range pending {
			if pending[i].EventTime.After(watermark) {
				afterWatermarkCount++
			}
		}
		newPending := make([]Record, afterWatermarkCount)
		for i := range pending {
			if pending[i].EventTime.After(watermark) {
				newPending = append(newPending, pending[i])
				crossedOut[i] = true
			}
		}

		lastIndex := -1
		for i := range pending {
			if !crossedOut[i] && pending[i].Retraction {
				lastIndex = i
			}
		}
		if lastIndex == -1 {
			log.Printf("last retraction: nil")
		} else {
			log.Printf("last retraction: %s", pending[lastIndex])
		}
		/*
			Ideowo ostatni rekord z danym timestampem to powinien być rekord, który miał po obu stronach ten timestamp, jeśli źródła są te same.
		*/

	pendingLoop:
		for i := range pending {
			if crossedOut[i] {
				continue
			}
			if !pending[i].Retraction {
				// Look if there is a retraction for this record, this is beautifully quadratic.
				// TODO: Optimize. Use a sensible data structure.
			findRetractionLoop:
				for j := i + 1; j < len(pending); j++ {
					if !pending[j].Retraction {
						continue
					}
					for k := range pending[i].Values {
						if pending[i].Values[k].Compare(pending[j].Values[k]) != 0 {
							continue findRetractionLoop
						}
					}
					crossedOut[i] = true
					crossedOut[j] = true
					continue pendingLoop
				}
			}
			if err := produce(ctx, pending[i]); err != nil {
				return fmt.Errorf("couldn't produce: %w", err)
			}
		}

		pending = newPending

		return nil
	}

	if err := node.Source.Run(
		ctx,
		func(ctx ProduceContext, record Record) error {
			// TODO: Periodic compaction. (by combining records with their retractions)
			pending = append(pending, record)
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			if msg.Type == MetadataMessageTypeWatermark {
				if err := sendPendingLessOrEqualWatermark(ctx, msg.Watermark); err != nil {
					return err
				}
			}

			if err := metaSend(ctx, msg); err != nil {
				return fmt.Errorf("couldn't send metadata: %w", err)
			}

			return nil
		},
	); err != nil {
		return err
	}

	return sendPendingLessOrEqualWatermark(ProduceFromExecutionContext(ctx), WatermarkMaxValue)
}

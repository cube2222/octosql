package nodes

import (
	"fmt"
	"time"

	"github.com/tidwall/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type GroupBy struct {
	aggregatePrototypes []func() Aggregate
	aggregateExprs      []Expression
	keyExprs            []Expression
	keyEventTimeIndex   int
	source              Node
	triggerPrototype    func() Trigger
}

func NewGroupBy(
	aggregatePrototypes []func() Aggregate,
	aggregateExprs []Expression,
	keyExprs []Expression,
	keyEventTimeIndex int,
	source Node,
	triggerPrototype func() Trigger,
) *GroupBy {
	return &GroupBy{
		aggregatePrototypes: aggregatePrototypes,
		aggregateExprs:      aggregateExprs,
		keyExprs:            keyExprs,
		keyEventTimeIndex:   keyEventTimeIndex,
		source: &EventTimeBuffer{
			source: source,
		},
		triggerPrototype: triggerPrototype,
	}
}

type Aggregate interface {
	Add(retraction bool, value octosql.Value) bool
	Trigger() octosql.Value
}

type aggregatesItem struct {
	GroupKey
	Aggregates []Aggregate

	// AggregatedSetSize omits NULL inputs.
	AggregatedSetSize []int

	// OverallRecordCount counts all records minus retractions.
	OverallRecordCount int
}

type previouslySentValuesItem struct {
	GroupKey
	Values    []octosql.Value
	EventTime time.Time
}

func (g *GroupBy) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	aggregates := btree.NewGenericOptions[*aggregatesItem](
		func(a, b *aggregatesItem) bool {
			return GroupKeyGenericLess(a.GroupKey, b.GroupKey)
		},
		btree.Options{
			NoLocks: true,
		},
	)
	previouslySentValues := btree.NewGenericOptions[*previouslySentValuesItem](
		func(a, b *previouslySentValuesItem) bool {
			return GroupKeyGenericLess(a.GroupKey, b.GroupKey)
		},
		btree.Options{
			NoLocks: true,
		},
	)
	trigger := g.triggerPrototype()

	if err := g.source.Run(ctx, func(produceCtx ProduceContext, record Record) error {
		ctx := ctx.WithRecord(record)

		key := make(GroupKey, len(g.keyExprs))
		for i, expr := range g.keyExprs {
			value, err := expr.Evaluate(ctx)
			if err != nil {
				return fmt.Errorf("couldn't evaluate %d group by key expression: %w", i, err)
			}
			key[i] = value
		}

		aggregateInputs := make([]octosql.Value, len(g.aggregateExprs))
		for i, expr := range g.aggregateExprs {
			value, err := expr.Evaluate(ctx)
			if err != nil {
				return fmt.Errorf("couldn't evaluate %d aggregate expression: %w", i, err)
			}
			aggregateInputs[i] = value
		}

		{
			item, ok := aggregates.Get(&aggregatesItem{GroupKey: key})

			if !ok {
				newAggregates := make([]Aggregate, len(g.aggregatePrototypes))
				for i := range g.aggregatePrototypes {
					newAggregates[i] = g.aggregatePrototypes[i]()
				}

				item = &aggregatesItem{GroupKey: key, Aggregates: newAggregates, AggregatedSetSize: make([]int, len(g.aggregatePrototypes))}
				aggregates.Set(item)
			}

			if !record.Retraction {
				item.OverallRecordCount++
			} else {
				item.OverallRecordCount--
			}
			for i, expr := range g.aggregateExprs {
				aggregateInput, err := expr.Evaluate(ctx)
				if err != nil {
					return fmt.Errorf("couldn't evaluate %d aggregate expression: %w", i, err)
				}

				if aggregateInput.TypeID != octosql.TypeIDNull {
					if !record.Retraction {
						item.AggregatedSetSize[i]++
					} else {
						item.AggregatedSetSize[i]--
					}
					item.Aggregates[i].Add(record.Retraction, aggregateInput)
				}
			}

			if item.OverallRecordCount == 0 {
				aggregates.Delete(item)
				// TODO: Also delete from triggers somehow? But have to force a retraction in that case.
			}

			trigger.KeyReceived(key)
		}

		if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previouslySentValues, trigger, record.EventTime, produce); err != nil {
			return fmt.Errorf("couldn't trigger keys on record receive")
		}

		return nil
	}, func(ctx ProduceContext, msg MetadataMessage) error {
		if msg.Type == MetadataMessageTypeWatermark {
			trigger.WatermarkReceived(msg.Watermark)
			if err := g.trigger(ctx, aggregates, previouslySentValues, trigger, msg.Watermark, produce); err != nil {
				return fmt.Errorf("couldn't trigger keys on watermark")
			}
		}
		return metaSend(ctx, msg)
	}); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	trigger.EndOfStreamReached()
	// TODO: What should be put here as the event time? WatermarkMaxValue kind of makes sense. But on the other hand, if this is then i.e. StreamJoin'ed with something then it would make everything MaxValue. But only if this is Batch. If it's grouping by event time then the event times will be correct.
	if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previouslySentValues, trigger, WatermarkMaxValue, produce); err != nil {
		return fmt.Errorf("couldn't trigger keys on end of stream")
	}

	return nil
}

func (g *GroupBy) trigger(produceCtx ProduceContext, aggregates *btree.Generic[*aggregatesItem], previouslySentValues *btree.Generic[*previouslySentValuesItem], trigger Trigger, curEventTime time.Time, produce ProduceFn) error {
	toTrigger := trigger.Poll()

	for _, key := range toTrigger {
		// Get values and produce, retracting previous values.
		newValueEventTime := curEventTime
		var outputValues []octosql.Value
		{
			// Get new record to send

			item, ok := aggregates.Get(&aggregatesItem{GroupKey: key})
			if ok {
				outputValues = make([]octosql.Value, len(key)+len(g.aggregateExprs))
				copy(outputValues, key)

				for i := range item.Aggregates {
					if item.AggregatedSetSize[i] > 0 {
						outputValues[len(key)+i] = item.Aggregates[i].Trigger()
					} else {
						outputValues[len(key)+i] = octosql.NewNull()
					}
				}

				// If the new record has a custom Event Time, then we use that for it and the possible corresponding retraction.
				if g.keyEventTimeIndex != -1 && newValueEventTime.After(outputValues[g.keyEventTimeIndex].Time) {
					newValueEventTime = outputValues[g.keyEventTimeIndex].Time
				}
			}
		}
		{
			// Send possible retraction

			item, ok := previouslySentValues.Delete(&previouslySentValuesItem{GroupKey: key})
			if ok {
				if err := produce(produceCtx, NewRecord(item.Values, true, newValueEventTime)); err != nil {
					return fmt.Errorf("couldn't produce: %w", err)
				}

				previouslySentValues.Delete(item)
			}
		}
		{
			// Send possible new value

			if outputValues != nil {
				if err := produce(produceCtx, NewRecord(outputValues, false, newValueEventTime)); err != nil {
					return fmt.Errorf("couldn't produce: %w", err)
				}

				previouslySentValues.Set(&previouslySentValuesItem{
					GroupKey:  key,
					Values:    outputValues,
					EventTime: newValueEventTime,
				})
			}
		}
	}

	return nil
}

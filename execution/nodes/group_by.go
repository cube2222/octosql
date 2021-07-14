package nodes

import (
	"fmt"
	"time"

	"github.com/google/btree"

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
		source:              source,
		triggerPrototype:    triggerPrototype,
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
}

type previouslySentValuesItem struct {
	GroupKey
	Values    []octosql.Value
	EventTime time.Time
}

func (g *GroupBy) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	aggregates := btree.New(BTreeDefaultDegree)
	previouslySentValues := btree.New(BTreeDefaultDegree)
	trigger := g.triggerPrototype()
	currentWatermark := time.Time{}

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
			item := aggregates.Get(key)
			var itemTyped *aggregatesItem

			if item == nil {
				newAggregates := make([]Aggregate, len(g.aggregatePrototypes))
				for i := range g.aggregatePrototypes {
					newAggregates[i] = g.aggregatePrototypes[i]()
				}

				itemTyped = &aggregatesItem{GroupKey: key, Aggregates: newAggregates, AggregatedSetSize: make([]int, len(g.aggregatePrototypes))}
				aggregates.ReplaceOrInsert(itemTyped)
			} else {
				var ok bool
				itemTyped, ok = item.(*aggregatesItem)
				if !ok {
					// TODO: Check performance cost of those panics.
					panic(fmt.Sprintf("invalid aggregates item: %v", item))
				}
			}

			for i, expr := range g.aggregateExprs {
				aggregateInput, err := expr.Evaluate(ctx)
				if err != nil {
					return fmt.Errorf("couldn't evaluate %d aggregate expression: %w", i, err)
				}

				if aggregateInput.Type.TypeID != octosql.TypeIDNull {
					if !record.Retraction {
						itemTyped.AggregatedSetSize[i]++
					} else {
						itemTyped.AggregatedSetSize[i]--
					}
					itemTyped.Aggregates[i].Add(record.Retraction, aggregateInput)
				}
			}

			// TODO: Delete entry if deletable.

			trigger.KeyReceived(key)
		}

		if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previouslySentValues, trigger, record.EventTime, produce); err != nil {
			return fmt.Errorf("couldn't trigger keys on end of stream")
		}

		return nil
	}, func(ctx ProduceContext, msg MetadataMessage) error {
		if msg.Type == MetadataMessageTypeWatermark {
			trigger.WatermarkReceived(msg.Watermark)
			currentWatermark = msg.Watermark
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

func (g *GroupBy) trigger(produceCtx ProduceContext, aggregates, previouslySentValues *btree.BTree, trigger Trigger, curEventTime time.Time, produce ProduceFn) error {
	toTrigger := trigger.Poll()

	for _, key := range toTrigger {
		// Get values and produce, retracting previous values.
		{
			item := previouslySentValues.Delete(key)
			if item != nil {
				itemTyped, ok := item.(*previouslySentValuesItem)
				if !ok {
					// TODO: Check performance cost of those panics.
					panic(fmt.Sprintf("invalid previously sent item: %v", item))
				}

				if err := produce(produceCtx, NewRecord(itemTyped.Values, true, itemTyped.EventTime)); err != nil {
					return fmt.Errorf("couldn't produce: %w", err)
				}

				previouslySentValues.Delete(key)
			}
		}
		{
			item := aggregates.Get(key)
			if item != nil {
				itemTyped, ok := item.(*aggregatesItem)
				if !ok {
					// TODO: Check performance cost of those panics.
					panic(fmt.Sprintf("invalid aggregates item: %v", item))
				}

				outputValues := make([]octosql.Value, len(key)+len(g.aggregateExprs))
				copy(outputValues, key)

				for i := range itemTyped.Aggregates {
					if itemTyped.AggregatedSetSize[i] > 0 {
						outputValues[len(key)+i] = itemTyped.Aggregates[i].Trigger()
					} else {
						outputValues[len(key)+i] = octosql.NewNull()
					}
				}

				eventTime := curEventTime
				if g.keyEventTimeIndex != -1 {
					eventTime = outputValues[g.keyEventTimeIndex].Time
				}

				if err := produce(produceCtx, NewRecord(outputValues, false, eventTime)); err != nil {
					return fmt.Errorf("couldn't produce: %w", err)
				}

				previouslySentValues.ReplaceOrInsert(&previouslySentValuesItem{
					GroupKey:  key,
					Values:    outputValues,
					EventTime: eventTime,
				})
			}
		}
	}

	return nil
}

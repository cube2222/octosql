package nodes

import (
	"time"

	"github.com/google/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type CustomTriggerGroupBy struct {
	aggregatePrototypes []func() Aggregate
	aggregateExprs      []Expression
	keyExprs            []Expression
	keyEventTimeIndex   int
	source              Node
	triggerPrototype    func() Trigger
}

func NewCustomTriggerGroupBy(
	aggregatePrototypes []func() Aggregate,
	aggregateExprs []Expression,
	keyExprs []Expression,
	keyEventTimeIndex int,
	source Node,
	triggerPrototype func() Trigger,
) *CustomTriggerGroupBy {
	return &CustomTriggerGroupBy{
		aggregatePrototypes: aggregatePrototypes,
		aggregateExprs:      aggregateExprs,
		keyExprs:            keyExprs,
		keyEventTimeIndex:   keyEventTimeIndex,
		source:              NewEventTimeBuffer(source),
		triggerPrototype:    triggerPrototype,
	}
}

type Aggregate interface {
	Add(retractions bool, values octosql.Value) bool
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

func (g *CustomTriggerGroupBy) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	panic("implement me")
	// aggregates := btree.New(BTreeDefaultDegree)
	// previouslySentValues := btree.New(BTreeDefaultDegree)
	// trigger := g.triggerPrototype()
	//
	// if err := g.source.Run(ctx, func(produceCtx ProduceContext, record RecordBatch) error {
	// 	ctx := ctx.WithRecord(record)
	//
	// 	key := make(GroupKey, len(g.keyExprs))
	// 	for i, expr := range g.keyExprs {
	// 		value, err := expr.Evaluate(ctx)
	// 		if err != nil {
	// 			return fmt.Errorf("couldn't evaluate %d group by key expression: %w", i, err)
	// 		}
	// 		key[i] = value
	// 	}
	//
	// 	aggregateInputs := make([]octosql.Value, len(g.aggregateExprs))
	// 	for i, expr := range g.aggregateExprs {
	// 		value, err := expr.Evaluate(ctx)
	// 		if err != nil {
	// 			return fmt.Errorf("couldn't evaluate %d aggregate expression: %w", i, err)
	// 		}
	// 		aggregateInputs[i] = value
	// 	}
	//
	// 	{
	// 		item := aggregates.Get(key)
	// 		var itemTyped *aggregatesItem
	//
	// 		if item == nil {
	// 			newAggregates := make([]Aggregate, len(g.aggregatePrototypes))
	// 			for i := range g.aggregatePrototypes {
	// 				newAggregates[i] = g.aggregatePrototypes[i]()
	// 			}
	//
	// 			itemTyped = &aggregatesItem{GroupKey: key, Aggregates: newAggregates, AggregatedSetSize: make([]int, len(g.aggregatePrototypes))}
	// 			aggregates.ReplaceOrInsert(itemTyped)
	// 		} else {
	// 			var ok bool
	// 			itemTyped, ok = item.(*aggregatesItem)
	// 			if !ok {
	// 				// TODO: Check performance cost of those panics.
	// 				panic(fmt.Sprintf("invalid aggregates item: %v", item))
	// 			}
	// 		}
	//
	// 		if !record.Retractions {
	// 			itemTyped.OverallRecordCount++
	// 		} else {
	// 			itemTyped.OverallRecordCount--
	// 		}
	// 		for i, expr := range g.aggregateExprs {
	// 			aggregateInput, err := expr.Evaluate(ctx)
	// 			if err != nil {
	// 				return fmt.Errorf("couldn't evaluate %d aggregate expression: %w", i, err)
	// 			}
	//
	// 			if aggregateInput.TypeID != octosql.TypeIDNull {
	// 				if !record.Retractions {
	// 					itemTyped.AggregatedSetSize[i]++
	// 				} else {
	// 					itemTyped.AggregatedSetSize[i]--
	// 				}
	// 				itemTyped.Aggregates[i].Add(record.Retractions, aggregateInput)
	// 			}
	// 		}
	//
	// 		if itemTyped.OverallRecordCount == 0 {
	// 			aggregates.Delete(itemTyped)
	// 			// TODO: Also delete from triggers somehow? But have to force a retraction in that case.
	// 		}
	//
	// 		trigger.KeyReceived(key)
	// 	}
	//
	// 	if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previouslySentValues, trigger, record.EventTimes, produce); err != nil {
	// 		return fmt.Errorf("couldn't trigger keys on record receive: %w", err)
	// 	}
	//
	// 	return nil
	// }, func(ctx ProduceContext, msg MetadataMessage) error {
	// 	if msg.Type == MetadataMessageTypeWatermark {
	// 		trigger.WatermarkReceived(msg.Watermark)
	// 		if err := g.trigger(ctx, aggregates, previouslySentValues, trigger, msg.Watermark, produce); err != nil {
	// 			return fmt.Errorf("couldn't trigger keys on watermark: %w", err)
	// 		}
	// 	}
	// 	return metaSend(ctx, msg)
	// }); err != nil {
	// 	return fmt.Errorf("couldn't run source: %w", err)
	// }
	//
	// trigger.EndOfStreamReached()
	// // TODO: What should be put here as the event time? WatermarkMaxValue kind of makes sense. But on the other hand, if this is then i.e. StreamJoin'ed with something then it would make everything MaxValue. But only if this is Batch. If it's grouping by event time then the event times will be correct.
	// if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previouslySentValues, trigger, WatermarkMaxValue, produce); err != nil {
	// 	return fmt.Errorf("couldn't trigger keys on end of stream: %w", err)
	// }
	//
	// return nil
}

func (g *CustomTriggerGroupBy) trigger(produceCtx ProduceContext, aggregates, previouslySentValues *btree.BTree, trigger Trigger, curEventTime time.Time, produce ProduceFn) error {
	panic("implement me")
	// toTrigger := trigger.Poll()
	//
	// for _, key := range toTrigger {
	// 	// Get values and produce, retracting previous values.
	// 	newValueEventTime := curEventTime
	// 	var outputValues []octosql.Value
	// 	{
	// 		// Get new record to send
	//
	// 		item := aggregates.Get(key)
	// 		if item != nil {
	// 			itemTyped, ok := item.(*aggregatesItem)
	// 			if !ok {
	// 				// TODO: Check performance cost of those panics.
	// 				panic(fmt.Sprintf("invalid aggregates item: %v", item))
	// 			}
	//
	// 			outputValues = make([]octosql.Value, len(key)+len(g.aggregateExprs))
	// 			copy(outputValues, key)
	//
	// 			for i := range itemTyped.Aggregates {
	// 				if itemTyped.AggregatedSetSize[i] > 0 {
	// 					outputValues[len(key)+i] = itemTyped.Aggregates[i].Trigger()
	// 				} else {
	// 					outputValues[len(key)+i] = octosql.NewNull()
	// 				}
	// 			}
	//
	// 			// If the new record has a custom Event Time, then we use that for it and the possible corresponding retraction.
	// 			if g.keyEventTimeIndex != -1 && newValueEventTime.After(outputValues[g.keyEventTimeIndex].Time) {
	// 				newValueEventTime = outputValues[g.keyEventTimeIndex].Time
	// 			}
	// 		}
	// 	}
	// 	{
	// 		// Send possible retraction
	//
	// 		item := previouslySentValues.Delete(key)
	// 		if item != nil {
	// 			itemTyped, ok := item.(*previouslySentValuesItem)
	// 			if !ok {
	// 				// TODO: Check performance cost of those panics.
	// 				panic(fmt.Sprintf("invalid previously sent item: %v", item))
	// 			}
	//
	// 			if err := produce(produceCtx, NewRecordBatch(itemTyped.Values, true, newValueEventTime)); err != nil {
	// 				return fmt.Errorf("couldn't produce: %w", err)
	// 			}
	// 		}
	// 	}
	// 	{
	// 		// Send possible new value
	//
	// 		if outputValues != nil {
	// 			if err := produce(produceCtx, NewRecordBatch(outputValues, false, newValueEventTime)); err != nil {
	// 				return fmt.Errorf("couldn't produce: %w", err)
	// 			}
	//
	// 			previouslySentValues.ReplaceOrInsert(&previouslySentValuesItem{
	// 				GroupKey:  key,
	// 				Values:    outputValues,
	// 				EventTime: newValueEventTime,
	// 			})
	// 		}
	// 	}
	// }
	//
	// return nil
}

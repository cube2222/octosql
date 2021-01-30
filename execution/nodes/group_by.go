package nodes

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql"
	. "github.com/cube2222/octosql/execution"
)

type GroupBy struct {
	aggregatePrototypes []func() Aggregate
	aggregateExprs      []Expression
	keyExprs            []Expression
	source              Node
	triggerPrototype    func() Trigger
}

type Aggregate interface {
	Add(retraction bool, value octosql.Value) bool
	Trigger() octosql.Value
}

type aggregatesItem struct {
	Key        GroupKey
	Aggregates []Aggregate
}

func (c *aggregatesItem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*aggregatesItem)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	return c.Key.Less(thanTyped.Key)
}

func (g *GroupBy) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	aggregates := btree.New(DefaultBTreeDegree)
	previousValues := btree.New(DefaultBTreeDegree)
	trigger := g.triggerPrototype()

	if err := g.source.Run(ctx, func(produceCtx ProduceContext, record Record) error {
		ctx := ctx.WithRecord(record)

		key := make([]octosql.Value, len(g.keyExprs))
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
			item := aggregates.Get(&aggregatesItem{Key: key})
			var itemTyped *aggregatesItem

			if item == nil {
				newAggregates := make([]Aggregate, len(g.aggregatePrototypes))
				for i := range g.aggregatePrototypes {
					newAggregates[i] = g.aggregatePrototypes[i]()
				}

				itemTyped = &aggregatesItem{Key: key, Aggregates: newAggregates}
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

				itemTyped.Aggregates[i].Add(record.Retraction, aggregateInput)
			}

			// TODO: Delete entry if deletable.

			trigger.KeyReceived(key)
		}

		if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previousValues, trigger, produce); err != nil {
			return fmt.Errorf("couldn't trigger keys on end of stream")
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	trigger.EndOfStreamReached()
	if err := g.trigger(ProduceFromExecutionContext(ctx), aggregates, previousValues, trigger, produce); err != nil {
		return fmt.Errorf("couldn't trigger keys on end of stream")
	}

	return nil
}

func (*GroupBy) trigger(produceCtx ProduceContext, aggregates, previousValues *btree.BTree, trigger Trigger, produce ProduceFn) error {
	toTrigger := trigger.Poll()

	for _, key := range toTrigger {
		// Get values and produce, retracing previous values.
		key = key
		if err := produce(produceCtx, NewRecord(nil)); err != nil {
			return fmt.Errorf("couldn't produce: %w", err)
		}
	}

	return nil
}

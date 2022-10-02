package nodes

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/zyedidia/generic/hashmap"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

// SimpleGroupBy is a special group by that's much faster than the CustomTriggerGroupBy but only works with no custom triggers.
type SimpleGroupBy struct {
	aggregatePrototypes []func() Aggregate
	aggregateExprs      []Expression
	keyExprs            []Expression
	source              Node
}

func NewSimpleGroupBy(
	aggregatePrototypes []func() Aggregate,
	aggregateExprs []Expression,
	keyExprs []Expression,
	source Node,
) *SimpleGroupBy {
	return &SimpleGroupBy{
		aggregatePrototypes: aggregatePrototypes,
		aggregateExprs:      aggregateExprs,
		keyExprs:            keyExprs,
		source:              source,
	}
}

type hashmapAggregatesItem struct {
	Aggregates []Aggregate

	// AggregatedSetSize omits NULL inputs.
	AggregatedSetSize []int

	// OverallRecordCount counts all records minus retractions.
	OverallRecordCount int
}

func (g *SimpleGroupBy) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	aggregates := hashmap.New[GroupKey, *hashmapAggregatesItem](BTreeDefaultDegree, func(a, b GroupKey) bool {
		for i := range a {
			if a[i].Compare(b[i]) != 0 {
				return false
			}
		}
		return true
	}, func(k GroupKey) uint64 {
		hash := fnv.New64()
		for _, v := range k {
			v.Hash(hash)
		}
		return hash.Sum64()
	})

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

		{
			itemTyped, ok := aggregates.Get(key)

			if !ok {
				newAggregates := make([]Aggregate, len(g.aggregatePrototypes))
				for i := range g.aggregatePrototypes {
					newAggregates[i] = g.aggregatePrototypes[i]()
				}

				itemTyped = &hashmapAggregatesItem{Aggregates: newAggregates, AggregatedSetSize: make([]int, len(g.aggregatePrototypes))}
				aggregates.Put(key, itemTyped)
			}

			if !record.Retraction {
				itemTyped.OverallRecordCount++
			} else {
				itemTyped.OverallRecordCount--
			}
			for i, expr := range g.aggregateExprs {
				aggregateInput, err := expr.Evaluate(ctx)
				if err != nil {
					return fmt.Errorf("couldn't evaluate %d aggregate expression: %w", i, err)
				}

				if aggregateInput.TypeID != octosql.TypeIDNull {
					if !record.Retraction {
						itemTyped.AggregatedSetSize[i]++
					} else {
						itemTyped.AggregatedSetSize[i]--
					}
					itemTyped.Aggregates[i].Add(record.Retraction, aggregateInput)
				}
			}

			if itemTyped.OverallRecordCount == 0 {
				aggregates.Remove(key)
			}
		}

		return nil
	}, func(ctx ProduceContext, msg MetadataMessage) error {
		return metaSend(ctx, msg)
	}); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	var err error
	func() {
		type stopEach struct{}
		defer func() {
			msg := recover()
			if msg == nil {
				return
			}
			if _, ok := msg.(stopEach); ok {
				return
			}
			panic(msg)
		}()
		aggregates.Each(func(key GroupKey, itemTyped *hashmapAggregatesItem) {
			outputValues := make([]octosql.Value, len(key)+len(g.aggregateExprs))
			copy(outputValues, key)

			for i := range itemTyped.Aggregates {
				if itemTyped.AggregatedSetSize[i] > 0 {
					outputValues[len(key)+i] = itemTyped.Aggregates[i].Trigger()
				} else {
					outputValues[len(key)+i] = octosql.NewNull()
				}
			}

			if err = produce(ProduceFromExecutionContext(ctx), NewRecord(outputValues, false, time.Time{})); err != nil {
				panic(stopEach{})
			}
		})
	}()

	return err
}

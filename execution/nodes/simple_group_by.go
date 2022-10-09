package nodes

import (
	"fmt"
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

type simplePreaggregatesItem struct {
	GroupKey
	AggregateValues [][]octosql.Value
	Retractions     []bool
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
		return octosql.HashManyValues(k)
	})

	if err := g.source.Run(ctx, func(produceCtx ProduceContext, records RecordBatch) error {
		ctx := ctx.WithRecord(records)
		defer func() {
			for i := range records.Values {
				ctx.SlicePool.Put(records.Values[i])
			}
		}()

		keyExprValues := make([][]octosql.Value, len(g.keyExprs))
		for i, expr := range g.keyExprs {
			value, err := expr.Evaluate(ctx)
			if err != nil {
				return fmt.Errorf("couldn't evaluate %d group by key expression: %w", i, err)
			}
			keyExprValues[i] = value
		}
		aggregateExprValues := make([][]octosql.Value, len(g.aggregateExprs))
		for i, expr := range g.aggregateExprs {
			value, err := expr.Evaluate(ctx)
			if err != nil {
				return fmt.Errorf("couldn't evaluate %d group by aggregate expression: %w", i, err)
			}
			aggregateExprValues[i] = value
		}

		for rowIndex := 0; rowIndex < records.Size; rowIndex++ {
			{
				key := Row(keyExprValues, rowIndex)
				itemTyped, ok := aggregates.Get(key)

				if !ok {
					newAggregates := make([]Aggregate, len(g.aggregatePrototypes))
					for i := range g.aggregatePrototypes {
						newAggregates[i] = g.aggregatePrototypes[i]()
					}

					itemTyped = &hashmapAggregatesItem{Aggregates: newAggregates, AggregatedSetSize: make([]int, len(g.aggregatePrototypes))}
					aggregates.Put(key, itemTyped)
				}

				if !records.Retractions[rowIndex] {
					itemTyped.OverallRecordCount++
				} else {
					itemTyped.OverallRecordCount--
				}
				for i := range g.aggregateExprs {
					if aggregateExprValues[i][rowIndex].TypeID != octosql.TypeIDNull {
						if !records.Retractions[rowIndex] {
							itemTyped.AggregatedSetSize[i]++
						} else {
							itemTyped.AggregatedSetSize[i]--
						}
						itemTyped.Aggregates[i].Add(records.Retractions[rowIndex], aggregateExprValues[i][rowIndex])
					}
				}

				if itemTyped.OverallRecordCount == 0 {
					aggregates.Remove(key)
				}
			}
		}

		return nil
	}, func(ctx ProduceContext, msg MetadataMessage) error {
		return metaSend(ctx, msg)
	}); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	outValues := make([][]octosql.Value, len(g.keyExprs)+len(g.aggregateExprs))
	for i := range outValues {
		outValues[i] = make([]octosql.Value, 0, DesiredBatchSize)
	}
	outValueCount := 0
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
			for i := range key {
				outValues[i] = append(outValues[i], key[i])
			}

			for i := range itemTyped.Aggregates {
				colIndex := len(g.keyExprs) + i
				if itemTyped.AggregatedSetSize[i] > 0 {
					outValues[colIndex] = append(outValues[colIndex], itemTyped.Aggregates[i].Trigger())
				} else {
					outValues[colIndex] = append(outValues[colIndex], octosql.NewNull())
				}
			}

			outValueCount++
			if outValueCount == DesiredBatchSize {
				if err = produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outValues, make([]bool, outValueCount), make([]time.Time, outValueCount))); err != nil {
					panic(stopEach{})
				}
				outValues = make([][]octosql.Value, len(g.keyExprs)+len(g.aggregateExprs))
				for i := range outValues {
					outValues[i] = make([]octosql.Value, 0, DesiredBatchSize)
				}
				outValueCount = 0
			}
		})
	}()
	if err != nil {
		return fmt.Errorf("couldn't produce: %w", err)
	}
	if outValueCount > 0 {
		if err = produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outValues, make([]bool, outValueCount), make([]time.Time, outValueCount))); err != nil {
			return fmt.Errorf("couldn't produce: %w", err)
		}
	}

	return err
}

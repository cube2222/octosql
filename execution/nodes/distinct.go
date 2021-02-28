package nodes

import (
	"fmt"

	"github.com/google/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type Distinct struct {
	source Node
}

func NewDistinct(source Node) *Distinct {
	return &Distinct{
		source: source,
	}
}

type distinctItem struct {
	Values []octosql.Value
	Count  int
}

func (item *distinctItem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*distinctItem)
	if !ok {
		panic(fmt.Sprintf("invalid distinct key comparison: %T", than))
	}

	for i := 0; i < len(item.Values); i++ {
		if comp := item.Values[i].Compare(thanTyped.Values[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

func (o *Distinct) Run(execCtx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	recordCounts := btree.New(BTreeDefaultDegree)
	o.source.Run(
		execCtx,
		func(ctx ProduceContext, record Record) error {
			item := recordCounts.Get(&distinctItem{Values: record.Values})
			var itemTyped *distinctItem
			if item == nil {
				itemTyped = &distinctItem{
					Values: record.Values,
					Count:  0,
				}
			} else {
				var ok bool
				itemTyped, ok = item.(*distinctItem)
				if !ok {
					panic(fmt.Sprintf("invalid order by item: %v", item))
				}
			}
			if !record.Retraction {
				itemTyped.Count++
			} else {
				itemTyped.Count--
			}
			if itemTyped.Count > 0 {
				// New record.
				if !record.Retraction && itemTyped.Count == 1 {
					if err := produce(ctx, record); err != nil {
						return fmt.Errorf("couldn't produce new record: %w", err)
					}
				}
				recordCounts.ReplaceOrInsert(itemTyped)
			} else {
				if err := produce(ctx, record); err != nil {
					return fmt.Errorf("couldn't retract record record: %w", err)
				}
				recordCounts.Delete(itemTyped)
			}
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			return nil
		},
	)

	return nil
}

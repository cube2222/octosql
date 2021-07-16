package nodes

import (
	"fmt"
	"time"

	"github.com/google/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

// TODO: Batch order by on endless stream should be statically forbidden.

type OrderBy struct {
	source               Node
	keyExprs             []Expression
	directionMultipliers []int
}

func NewBatchOrderBy(source Node, keyExprs []Expression, directionMultipliers []int) *OrderBy {
	return &OrderBy{
		source:               source,
		keyExprs:             keyExprs,
		directionMultipliers: directionMultipliers,
	}
}

type orderByItem struct {
	Key                  []octosql.Value
	Values               []octosql.Value
	Count                int
	DirectionMultipliers []int
}

func (item *orderByItem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*orderByItem)
	if !ok {
		panic(fmt.Sprintf("invalid order by key comparison: %T", than))
	}

	for i := 0; i < len(item.Key); i++ {
		if comp := item.Key[i].Compare(thanTyped.Key[i]); comp != 0 {
			return comp*item.DirectionMultipliers[i] == -1
		}
	}

	// If keys are equal, differentiate by values.
	for i := 0; i < len(item.Values); i++ {
		if comp := item.Values[i].Compare(thanTyped.Values[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

func (o *OrderBy) Run(execCtx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	recordCounts := btree.New(BTreeDefaultDegree)
	o.source.Run(
		execCtx,
		func(ctx ProduceContext, record Record) error {
			key := make([]octosql.Value, len(o.keyExprs))
			for i := range o.keyExprs {
				keyValue, err := o.keyExprs[i].Evaluate(execCtx.WithRecord(record))
				if err != nil {
					return fmt.Errorf("couldn't evaluate order by %d key expression: %w", i, err)
				}
				key[i] = keyValue
			}

			item := recordCounts.Get(&orderByItem{Key: key, Values: record.Values, DirectionMultipliers: o.directionMultipliers})
			var itemTyped *orderByItem
			if item == nil {
				itemTyped = &orderByItem{
					Key:                  key,
					Values:               record.Values,
					Count:                0,
					DirectionMultipliers: o.directionMultipliers,
				}
			} else {
				var ok bool
				itemTyped, ok = item.(*orderByItem)
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
				recordCounts.ReplaceOrInsert(itemTyped)
			} else {
				recordCounts.Delete(itemTyped)
			}
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			return nil
		},
	)

	if err := produceOrderByItems(ProduceFromExecutionContext(execCtx), recordCounts, produce); err != nil {
		return fmt.Errorf("couldn't produce ordered items: %w", err)
	}
	return nil
}

func produceOrderByItems(ctx ProduceContext, recordCounts *btree.BTree, produce ProduceFn) error {
	var outErr error
	recordCounts.Ascend(func(item btree.Item) bool {
		itemTyped, ok := item.(*orderByItem)
		if !ok {
			panic(fmt.Sprintf("invalid order by item: %v", item))
		}
		for i := 0; i < itemTyped.Count; i++ {
			if err := produce(ctx, NewRecord(itemTyped.Values, false, time.Time{})); err != nil {
				outErr = err
				return false
			}
		}
		return true
	})
	return outErr
}

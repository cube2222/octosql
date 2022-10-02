package nodes

import (
	"fmt"

	"github.com/google/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type OrderSensitiveTransform struct {
	source                      Node
	orderByKeyExprs             []Expression
	orderByDirectionMultipliers []int
	limit                       *Expression
	noRetractionsPossible       bool
}

func NewOrderSensitiveTransform(source Node, orderByKeyExprs []Expression, orderByDirectionMultipliers []int, limit *Expression, noRetractionsPossible bool) *OrderSensitiveTransform {
	return &OrderSensitiveTransform{
		source:                      source,
		orderByKeyExprs:             orderByKeyExprs,
		orderByDirectionMultipliers: orderByDirectionMultipliers,
		limit:                       limit,
		noRetractionsPossible:       noRetractionsPossible,
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

func (o *OrderSensitiveTransform) Run(execCtx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	panic("implement me")
	// var limit *int
	// if o.limit != nil {
	// 	val, err := (*o.limit).Evaluate(execCtx)
	// 	if err != nil {
	// 		return fmt.Errorf("couldn't evaluate limit: %w", err)
	// 	}
	// 	if val.Int == 0 {
	// 		return nil
	// 	}
	// 	if val.Int < 0 {
	// 		return fmt.Errorf("limit must be positive, got %d", val.Int)
	// 	}
	// 	limit = &val.Int
	// }
	//
	// recordCounts := btree.New(BTreeDefaultDegree)
	// o.source.Run(
	// 	execCtx,
	// 	func(ctx ProduceContext, record RecordBatch) error {
	// 		key := make([]octosql.Value, len(o.orderByKeyExprs))
	// 		for i := range o.orderByKeyExprs {
	// 			keyValue, err := o.orderByKeyExprs[i].Evaluate(execCtx.WithRecord(record))
	// 			if err != nil {
	// 				return fmt.Errorf("couldn't evaluate order by %d key expression: %w", i, err)
	// 			}
	// 			key[i] = keyValue
	// 		}
	//
	// 		item := recordCounts.Get(&orderByItem{Key: key, Values: record.Values, DirectionMultipliers: o.orderByDirectionMultipliers})
	// 		var itemTyped *orderByItem
	// 		if item == nil {
	// 			itemTyped = &orderByItem{
	// 				Key:                  key,
	// 				Values:               record.Values,
	// 				Count:                0,
	// 				DirectionMultipliers: o.orderByDirectionMultipliers,
	// 			}
	// 		} else {
	// 			var ok bool
	// 			itemTyped, ok = item.(*orderByItem)
	// 			if !ok {
	// 				panic(fmt.Sprintf("invalid order by item: %v", item))
	// 			}
	// 		}
	// 		if !record.Retractions {
	// 			itemTyped.Count++
	// 		} else {
	// 			itemTyped.Count--
	// 		}
	// 		if itemTyped.Count > 0 {
	// 			recordCounts.ReplaceOrInsert(itemTyped)
	// 		} else {
	// 			recordCounts.Delete(itemTyped)
	// 		}
	// 		if limit != nil && o.noRetractionsPossible && recordCounts.Len() > *limit {
	// 			// This doesn't mean we'll always keep just the records that are needed, because tree nodes might have count > 1.
	// 			// That said, it's a good approximation, and we'll definitely not lose something that we need to have.
	// 			recordCounts.DeleteMax()
	// 		}
	// 		return nil
	// 	},
	// 	func(ctx ProduceContext, msg MetadataMessage) error {
	// 		return nil
	// 	},
	// )
	//
	// if err := produceOrderByItems(ProduceFromExecutionContext(execCtx), recordCounts, limit, produce); err != nil {
	// 	return fmt.Errorf("couldn't produce ordered items: %w", err)
	// }
	// return nil
}

func produceOrderByItems(ctx ProduceContext, recordCounts *btree.BTree, limit *int, produce ProduceFn) error {
	panic("implement me")
	// i := 0
	// var outErr error
	// recordCounts.Ascend(func(item btree.Item) bool {
	// 	if limit != nil && i >= *limit {
	// 		return false
	// 	}
	// 	i++
	// 	itemTyped, ok := item.(*orderByItem)
	// 	if !ok {
	// 		panic(fmt.Sprintf("invalid order by item: %v", item))
	// 	}
	// 	for i := 0; i < itemTyped.Count; i++ {
	// 		if err := produce(ctx, NewRecordBatch(itemTyped.Values, false, time.Time{})); err != nil {
	// 			outErr = err
	// 			return false
	// 		}
	// 	}
	// 	return true
	// })
	// return outErr
}

package nodes

import (
	"fmt"

	"github.com/tidwall/btree"

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

func (o *Distinct) Run(execCtx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	recordCounts := btree.NewGenericOptions(func(item, than *distinctItem) bool {
		for i := 0; i < len(item.Values); i++ {
			if comp := item.Values[i].Compare(than.Values[i]); comp != 0 {
				return comp == -1
			}
		}

		return false
	}, btree.Options{
		NoLocks: true,
	})
	o.source.Run(
		execCtx,
		func(ctx ProduceContext, record Record) error {
			item, ok := recordCounts.Get(&distinctItem{Values: record.Values})
			if !ok {
				item = &distinctItem{
					Values: record.Values,
					Count:  0,
				}
			}
			if !record.Retraction {
				item.Count++
			} else {
				item.Count--
			}
			if item.Count > 0 {
				// New record.
				if !record.Retraction && item.Count == 1 {
					if err := produce(ctx, record); err != nil {
						return fmt.Errorf("couldn't produce new record: %w", err)
					}
					recordCounts.Set(item)
				}
			} else {
				if err := produce(ctx, record); err != nil {
					return fmt.Errorf("couldn't retract record record: %w", err)
				}
				recordCounts.Delete(item)
			}
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			return nil
		},
	)

	return nil
}

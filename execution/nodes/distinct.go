package nodes

import (
	"fmt"

	"github.com/zyedidia/generic/hashmap"

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
	Count int
}

func (o *Distinct) Run(execCtx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	recordCounts := hashmap.New[[]octosql.Value, *distinctItem](
		BTreeDefaultDegree,
		func(a, b []octosql.Value) bool {
			for i := range a {
				if a[i].Compare(b[i]) != 0 {
					return false
				}
			}
			return true
		}, func(k []octosql.Value) uint64 {
			return octosql.HashManyValues(k)
		})
	o.source.Run(
		execCtx,
		func(ctx ProduceContext, record Record) error {
			item, ok := recordCounts.Get(record.Values)
			if !ok {
				item = &distinctItem{
					Count: 0,
				}
			}
			if !record.Retraction {
				item.Count++
			} else {
				item.Count--
			}
			if item.Count > 0 {
				if !record.Retraction && item.Count == 1 {
					// New record.
					if err := produce(ctx, record); err != nil {
						return fmt.Errorf("couldn't produce new record: %w", err)
					}
					recordCounts.Put(record.Values, item)
				}
			} else {
				if err := produce(ctx, record); err != nil {
					return fmt.Errorf("couldn't retract record record: %w", err)
				}
				recordCounts.Remove(record.Values)
			}
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			return nil
		},
	)

	return nil
}

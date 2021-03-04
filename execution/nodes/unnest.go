package nodes

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

// TODO: Map -> Unnest

type Unnest struct {
	source Node
	index  int
}

func NewUnnest(source Node, index int) *Unnest {
	return &Unnest{source: source, index: index}
}

func (u *Unnest) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	return u.source.Run(ctx, func(ctx ProduceContext, record Record) error {
		list := record.Values[u.index].List
		for i := range list {
			values := make([]octosql.Value, len(record.Values))
			copy(values, record.Values[:u.index])
			values[u.index] = list[i]
			if u.index < len(record.Values)-1 {
				copy(values[u.index+1:], record.Values[u.index+1:])
			}
			if err := produce(ctx, NewRecord(values, record.Retraction)); err != nil {
				return fmt.Errorf("couldn't produce unnested record: %w", err)
			}
		}
		return nil
	}, metaSend)
}

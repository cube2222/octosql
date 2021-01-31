package nodes

import (
	"fmt"

	"github.com/cube2222/octosql"
	. "github.com/cube2222/octosql/execution"
)

type Map struct {
	source Node
	exprs  []Expression
}

func NewMap(source Node, exprs []Expression) *Map {
	return &Map{
		source: source,
		exprs:  exprs,
	}
}

func (m *Map) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	if err := m.source.Run(ctx, func(produceCtx ProduceContext, record Record) error {
		ctx := ctx.WithRecord(record)

		// TODO: Reuse this slice on every produce call? NO, because of stream join for instance.
		values := make([]octosql.Value, len(m.exprs))
		for i, expr := range m.exprs {
			value, err := expr.Evaluate(ctx)
			if err != nil {
				return fmt.Errorf("couldn't evaluate %d map expression: %w", i, err)
			}
			values[i] = value
		}
		if err := produce(produceCtx, NewRecord(values, record.Retraction)); err != nil {
			return fmt.Errorf("couldn't produce: %w", err)
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}
	return nil
}

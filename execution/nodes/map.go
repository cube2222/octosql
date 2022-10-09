package nodes

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
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
	if err := m.source.Run(ctx, func(produceCtx ProduceContext, record RecordBatch) error {
		ctx := ctx.WithRecord(record)

		// TODO: Reuse this slice on every produce call? NO, because of stream join for instance.
		values := make([][]octosql.Value, len(m.exprs))
		for i, expr := range m.exprs {
			curValues, err := expr.Evaluate(ctx)
			if err != nil {
				return fmt.Errorf("couldn't evaluate %d map expression: %w", i, err)
			}
			values[i] = curValues
		}
		if err := produce(produceCtx, NewRecordBatch(values, record.Retractions, record.EventTimes)); err != nil {
			return fmt.Errorf("couldn't produce: %w", err)
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}
	return nil
}

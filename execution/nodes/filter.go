package nodes

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type Filter struct {
	source    Node
	predicate Expression
}

func NewFilter(source Node, predicate Expression) *Filter {
	return &Filter{
		source:    source,
		predicate: predicate,
	}
}

func (m *Filter) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	if err := m.source.Run(ctx, func(produceCtx ProduceContext, record Record) error {
		ctx := ctx.WithRecord(record)

		ok, err := m.predicate.Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate condition: %w", err)
		}
		if ok.Type.TypeID == octosql.TypeIDBoolean && ok.Boolean {
			if err := produce(produceCtx, record); err != nil {
				return fmt.Errorf("couldn't produce: %w", err)
			}
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}
	return nil
}

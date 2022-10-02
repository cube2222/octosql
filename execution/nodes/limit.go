package nodes

import (
	. "github.com/cube2222/octosql/execution"
)

type Limit struct {
	source Node
	limit  Expression
}

func NewLimit(source Node, limit Expression) *Limit {
	return &Limit{
		source: source,
		limit:  limit,
	}
}

func (m *Limit) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	panic("implement me")
	// limit, err := m.limit.Evaluate(ctx)
	// if err != nil {
	// 	return fmt.Errorf("couldn't evaluate limit expression: %w", err)
	// }
	//
	// limitNodeID := ulid.MustNew(ulid.Now(), rand.Reader).String()
	//
	// i := 0
	// if err := m.source.Run(ctx, func(produceCtx ProduceContext, record RecordBatch) error {
	// 	if err := produce(produceCtx, record); err != nil {
	// 		return fmt.Errorf("couldn't produce: %w", err)
	// 	}
	// 	i++
	//
	// 	if i == limit.Int {
	// 		// This error is returned because the limit has been reached, to stop underlying processing.
	// 		// It will be caught and silenced by the Limit node that emitted it.
	// 		return fmt.Errorf("limit %s reached", limitNodeID)
	// 	}
	//
	// 	return nil
	// }, metaSend); err != nil {
	// 	// We can't Unwrap because gRPC doesn't propagate wrapped errors, so we can't Unwrap over the plugin barrier.
	// 	if strings.Contains(err.Error(), fmt.Sprintf("limit %s reached", limitNodeID)) {
	// 		return nil
	// 	}
	// 	return fmt.Errorf("couldn't run source: %w", err)
	// }
	// return nil
}

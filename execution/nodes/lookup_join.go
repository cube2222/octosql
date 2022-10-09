package nodes

import (
	. "github.com/cube2222/octosql/execution"
)

type LookupJoin struct {
	source, joined Node
}

func NewLookupJoin(source, joined Node) *LookupJoin {
	return &LookupJoin{
		source: source,
		joined: joined,
	}
}

func (s *LookupJoin) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	// TODO: Add parallelism here.

	// TODO: Maybe this should be fully parallel with no ordering guarantees and maybe a parallelism limit (semaphore). This way it would kinda work when the joined stream has a time field.
	panic("implement me")
	// if err := s.source.Run(ctx, func(produceCtx ProduceContext, sourceRecord RecordBatch) error {
	// 	ctx := ctx.WithRecord(sourceRecord)
	//
	// 	if err := s.joined.Run(ctx, func(produceCtx ProduceContext, joinedRecord RecordBatch) error {
	// 		outputValues := make([]octosql.Value, len(sourceRecord.Values)+len(joinedRecord.Values))
	//
	// 		copy(outputValues, sourceRecord.Values)
	// 		copy(outputValues[len(sourceRecord.Values):], joinedRecord.Values)
	//
	// 		retraction := (sourceRecord.Retractions || joinedRecord.Retractions) && !(sourceRecord.Retractions && joinedRecord.Retractions)
	//
	// 		if err := produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outputValues, retraction, sourceRecord.EventTimes)); err != nil {
	// 			return fmt.Errorf("couldn't produce: %w", err)
	// 		}
	//
	// 		return nil
	// 	}, metaSend); err != nil {
	// 		return fmt.Errorf("couldn't run joined stream: %w", err)
	// 	}
	//
	// 	return nil
	// }, metaSend); err != nil {
	// 	return fmt.Errorf("couldn't run source: %w", err)
	// }
	// return nil
}

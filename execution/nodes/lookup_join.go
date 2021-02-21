package nodes

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
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
	parallelism := 64
	sourceMetadataChan := make(chan MetadataMessage)
	sourceRecordsChan := make(chan Record)
	semaphore := make(chan struct{}, parallelism)

	if err := s.source.Run(ctx, func(produceCtx ProduceContext, sourceRecord Record) error {
		ctx := ctx.WithRecord(sourceRecord)

		if err := s.joined.Run(ctx, func(produceCtx ProduceContext, joinedRecord Record) error {
			outputValues := make([]octosql.Value, len(sourceRecord.Values)+len(joinedRecord.Values))

			copy(outputValues, sourceRecord.Values)
			copy(outputValues[len(sourceRecord.Values):], joinedRecord.Values)

			retraction := (sourceRecord.Retraction || joinedRecord.Retraction) && !(sourceRecord.Retraction && joinedRecord.Retraction)

			if err := produce(ProduceFromExecutionContext(ctx), NewRecord(outputValues, retraction)); err != nil {
				return fmt.Errorf("couldn't produce: %w", err)
			}

			return nil
		}, metaSend); err != nil {
			return fmt.Errorf("couldn't run joined stream: %w", err)
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	i := 0
	lastSent := -1
	records := make([]Record, parallelism)
	sourceRecords := make([]Record, parallelism)
	done := make([]bool, parallelism)
	joinedRecords := make([][]Record, parallelism)
	finishedGoroutine := make(chan struct{})
	// When we receive a message on the done channel,
	// we send the whole prefix of sourceRecords which are marked as done.

	return nil
}

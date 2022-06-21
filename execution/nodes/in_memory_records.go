package nodes

import (
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type InMemoryRecords struct {
	records []execution.Record
}

func NewInMemoryRecords(records []execution.Record) *InMemoryRecords {
	return &InMemoryRecords{
		records: records,
	}
}

func (r *InMemoryRecords) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	for i := 0; i < len(r.records); i++ {
		recordValues := make([]octosql.Value, len(r.records[i].Values))
		copy(recordValues, r.records[i].Values)

		if err := produce(
			execution.ProduceFromExecutionContext(ctx),
			execution.NewRecord(recordValues, r.records[i].Retraction, r.records[i].EventTime),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}

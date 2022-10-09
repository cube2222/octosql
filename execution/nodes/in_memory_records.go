package nodes

import (
	"github.com/cube2222/octosql/execution"
)

type InMemoryRecords struct {
	records []execution.RecordBatch
}

func NewInMemoryRecords(records []execution.RecordBatch) *InMemoryRecords {
	return &InMemoryRecords{
		records: records,
	}
}

func (r *InMemoryRecords) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	panic("implement me")
	// for i := 0; i < len(r.records); i++ {
	// 	recordValues := make([]octosql.Value, len(r.records[i].Values))
	// 	copy(recordValues, r.records[i].Values)
	//
	// 	if err := produce(
	// 		execution.ProduceFromExecutionContext(ctx),
	// 		execution.NewRecordBatch(recordValues, r.records[i].Retractions, r.records[i].EventTimes),
	// 	); err != nil {
	// 		return fmt.Errorf("couldn't produce record: %w", err)
	// 	}
	// }
	// return nil
}

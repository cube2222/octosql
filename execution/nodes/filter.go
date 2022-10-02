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
	var outAccumulator RecordBatch
	if err := m.source.Run(ctx, func(produceCtx ProduceContext, records RecordBatch) error {
		ctx := ctx.WithRecord(records)

		if outAccumulator.Size == 0 {
			outAccumulator.Values = make([][]octosql.Value, len(records.Values))
			for i := range outAccumulator.Values {
				outAccumulator.Values[i] = make([]octosql.Value, 0, DesiredBatchSize)
			}
		}

		ok, err := m.predicate.Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate condition: %w", err)
		}
		okBoolean := make([]bool, len(ok))
		newCount := 0
		for rowIndex := range ok {
			okBoolean[rowIndex] = ok[rowIndex].TypeID == octosql.TypeIDBoolean && ok[rowIndex].Boolean
			newCount++
		}
		for colIndex := range records.Values {
			for rowIndex := range records.Values[colIndex] {
				if okBoolean[rowIndex] {
					outAccumulator.Values[colIndex] = append(outAccumulator.Values[colIndex], records.Values[colIndex][rowIndex])
				}
			}
		}
		for rowIndex := range records.Retractions {
			if okBoolean[rowIndex] {
				outAccumulator.Retractions = append(outAccumulator.Retractions, records.Retractions[rowIndex])
			}
		}
		for rowIndex := range records.EventTimes {
			if okBoolean[rowIndex] {
				outAccumulator.EventTimes = append(outAccumulator.EventTimes, records.EventTimes[rowIndex])
			}
		}
		outAccumulator.Size += newCount

		if outAccumulator.Size > int(DesiredBatchSize/4)*3 {
			if err := produce(produceCtx, outAccumulator); err != nil {
				return fmt.Errorf("couldn't produce: %w", err)
			}
			outAccumulator = RecordBatch{}
		}

		return nil
	}, func(produceCtx ProduceContext, msg MetadataMessage) error {
		if outAccumulator.Size > 0 {
			if err := produce(produceCtx, outAccumulator); err != nil {
				return fmt.Errorf("couldn't produce: %w", err)
			}
			outAccumulator = RecordBatch{}
		}
		return metaSend(produceCtx, msg)
	}); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}
	if outAccumulator.Size > 0 {
		if err := produce(ProduceFromExecutionContext(ctx), outAccumulator); err != nil {
			return fmt.Errorf("couldn't produce: %w", err)
		}
	}
	return nil
}

package nodes

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cube2222/octosql/arrowexec/execution"
)

type Map struct {
	OutSchema *arrow.Schema
	Source    *execution.NodeWithMeta

	Exprs []execution.Expression
}

func (m *Map) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	return m.Source.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
		outCols := make([]arrow.Array, len(m.Exprs))
		for i := range outCols { // TODO: Parallelize.
			arr, err := m.Exprs[i].Evaluate(produceCtx.Context, record)
			if err != nil {
				return fmt.Errorf("couldn't evaluate expression: %w", err)
			}
			outCols[i] = arr
		}

		if err := produce(produceCtx, execution.Record{Record: array.NewRecord(m.OutSchema, outCols, record.NumRows())}); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
		return nil
	})
}

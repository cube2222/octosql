package nodes

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/arrowexec/helpers"
	"golang.org/x/sync/errgroup"
)

// Here there are two Filter implementations.
// The NaiveFilter just uses the arrow library function.
// Its advantage is that it supports all formats of data and is
// a bit (~1.4x) faster if most of the rows are filtered out.
// The RebatchingFilter has a custom routine for filtering records, it
// re-batches the filtered records so that they aren't too far off the
// ideal batch size.
// It actually ends up being *much* (~3x) faster if only few
// records are being filtered out.
// The break-even point for some naive integer arrays is at ~3.5% of records
// being filtered out.
//
// It's interesting, cause the original idea for the re-batching was
// that downstream operators should be faster if batches aren't too small.
// However, with most of the records filtered out, the workload for downstream
// operators is so small that it doesn't really matter.

// NaiveFilter uses the arrow libraries selection function.
type NaiveFilter struct {
	source    *execution.NodeWithMeta
	predicate execution.Expression
}

func (f *NaiveFilter) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	return f.source.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
		selection, err := f.predicate.Evaluate(produceCtx.Context, record)
		if err != nil {
			return fmt.Errorf("couldn't evaluate filter predicate: %w", err)
		}

		out, err := compute.FilterRecordBatch(ctx.Context, record, selection, &compute.FilterOptions{
			NullSelection: compute.SelectionDropNulls,
		})
		if err != nil {
			return fmt.Errorf("couldn't filter record batch: %w", err)
		}

		if err := produce(produceCtx, execution.Record{Record: out}); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}

		return nil
	})
}

// RebatchingFilter has a custom routine for filtering records, it re-batches the filtered records so that they aren't too far off the ideal batch size.
type RebatchingFilter struct {
	source    *execution.NodeWithMeta
	predicate execution.Expression
}

func NewFilter(source *execution.NodeWithMeta, predicate execution.Expression) *RebatchingFilter {
	return &RebatchingFilter{
		source:    source,
		predicate: predicate,
	}
}

func (f *RebatchingFilter) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), f.source.Schema) // TODO: Get allocator as argument.
	if err := f.source.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
		selection, err := f.predicate.Evaluate(produceCtx.Context, record)
		if err != nil {
			return fmt.Errorf("couldn't evaluate filter predicate: %w", err)
		}
		typedSelection := selection.(*array.Boolean)

		g, _ := errgroup.WithContext(ctx.Context)
		columns := record.Columns()
		for i, column := range columns {
			rewriter := helpers.MakeColumnRewriter(recordBuilder.Field(i), column)
			g.Go(func() error {
				Rewrite(typedSelection, rewriter)
				return nil
			})
		}
		g.Wait()

		// TODO: What if there are no fields...? This is a case that's generally unhandled right now everywhere. Need to add a count to the record struct.
		if recordBuilder.Field(0).Len() > execution.IdealBatchSize/2 {
			outRecord := recordBuilder.NewRecord()
			if err := produce(produceCtx, execution.Record{Record: outRecord}); err != nil {
				return fmt.Errorf("couldn't produce record: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("couldn't run source node: %w", err)
	}

	if recordBuilder.Field(0).Len() > 0 {
		outRecord := recordBuilder.NewRecord()
		if err := produce(execution.ProduceContext{Context: ctx}, execution.Record{Record: outRecord}); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}

func Rewrite(selection *array.Boolean, rewriteFunc func(rowIndex int)) {
	for i := 0; i < selection.Len(); i++ {
		if selection.Value(i) {
			rewriteFunc(i)
		}
	}
}

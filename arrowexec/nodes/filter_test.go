package nodes

import (
	"context"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cube2222/octosql/arrowexec/execution"
)

// selectivity as a tenth of a percent (so 1000 means 100%)
const selectivity = 950
const rounds = 1024

var predicateArr = func() arrow.Array {
	predicateBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
	for i := 0; i < execution.IdealBatchSize; i++ {
		if rand.Intn(1000) < selectivity {
			predicateBuilder.Append(true)
		} else {
			predicateBuilder.Append(false)
		}
	}
	return predicateBuilder.NewArray()
}()

func BenchmarkNaiveFilter(b *testing.B) {
	b.StopTimer()
	groupBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	for i := 0; i < execution.IdealBatchSize; i++ {
		groupBuilder.Append(1)
	}
	groupArr := groupBuilder.NewArray()
	numbersBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	for i := 0; i < execution.IdealBatchSize; i++ {
		numbersBuilder.Append(int64(i))
	}
	numbersArr := numbersBuilder.NewArray()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	var node execution.NodeWithMeta
	node = execution.NodeWithMeta{
		Node: &TestNode{
			Records:     []execution.Record{{Record: array.NewRecord(schema, []arrow.Array{groupArr, numbersArr}, execution.IdealBatchSize)}},
			Repetitions: rounds,
		},
		Schema: schema,
	}
	node = execution.NodeWithMeta{
		Node: &NaiveFilter{
			Source: node,
			Predicate: &execution.ConstArray{
				Array: predicateArr,
			},
		},
		Schema: schema,
	}
	node = execution.NodeWithMeta{
		Node: &GroupBy{
			OutSchema:             schema,
			Source:                node,
			KeyColumns:            []int{0},
			AggregateConstructors: []func(dt arrow.DataType) Aggregate{MakeCount},
			AggregateColumns:      []int{1},
		},
		Schema: schema,
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		var outRecords []execution.Record
		var count int64
		if err := node.Node.Run(execution.Context{Context: context.Background()}, func(produceCtx execution.ProduceContext, record execution.Record) error {
			// log.Println(record)
			outRecords = append(outRecords, record)
			count += record.NumRows()
			return nil
		}); err != nil {
			panic(err)
		}
		outRecords = outRecords
		// log.Println("naive count:", count)
	}
}

func BenchmarkRebatchingFilter(b *testing.B) {
	b.StopTimer()
	groupBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	for i := 0; i < execution.IdealBatchSize; i++ {
		groupBuilder.Append(1)
	}
	groupArr := groupBuilder.NewArray()
	numbersBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	for i := 0; i < execution.IdealBatchSize; i++ {
		numbersBuilder.Append(int64(i))
	}
	numbersArr := numbersBuilder.NewArray()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	var node execution.NodeWithMeta
	node = execution.NodeWithMeta{
		Node: &TestNode{
			Records:     []execution.Record{{Record: array.NewRecord(schema, []arrow.Array{groupArr, numbersArr}, execution.IdealBatchSize)}},
			Repetitions: rounds,
		},
		Schema: schema,
	}
	node = execution.NodeWithMeta{
		Node: &RebatchingFilter{
			source: node,
			predicate: &execution.ConstArray{
				Array: predicateArr,
			},
		},
		Schema: schema,
	}
	// node = execution.NodeWithMeta{
	// 	Node: &GroupBy{
	// 		OutSchema:             schema,
	// 		Source:                node,
	// 		KeyColumns:            []int{0},
	// 		AggregateConstructors: []func(dt arrow.DataType) Aggregate{MakeCount},
	// 		AggregateColumns:      []int{1},
	// 	},
	// 	Schema: schema,
	// }
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		var outRecords []execution.Record
		var count int64
		if err := node.Node.Run(execution.Context{Context: context.Background()}, func(produceCtx execution.ProduceContext, record execution.Record) error {
			// log.Println(record)
			outRecords = append(outRecords, record)
			count += record.NumRows()
			return nil
		}); err != nil {
			panic(err)
		}
		outRecords = outRecords
		// log.Println("rebatching count:", count)
	}
}

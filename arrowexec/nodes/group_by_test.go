package nodes

import (
	"context"
	"log"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cube2222/octosql/arrowexec/execution"
)

func TestGroupBy(t *testing.T) {
	ctx := context.Background()
	allocator := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	aBuilder := array.NewInt64Builder(allocator)
	bBuilder := array.NewInt64Builder(allocator)

	var records []execution.Record

	for i := 0; i < 3; i++ {
		for j := 0; j < 4; j++ {
			aBuilder.Append(int64((i + j) % 3))
			bBuilder.Append(int64((i + 1) * j))
		}
		records = append(records, execution.Record{
			Record: array.NewRecord(schema, []arrow.Array{aBuilder.NewArray(), bBuilder.NewArray()}, 4),
		})
	}

	var node execution.NodeWithMeta
	node = execution.NodeWithMeta{
		Node: &TestNode{
			Records: records,
		},
		Schema: schema,
	}
	node = execution.NodeWithMeta{
		Node: &GroupBy{
			OutSchema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					{Name: "b_sum", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				},
				nil,
			),
			Source: node,

			KeyColumns:       []int{0},
			AggregateColumns: []int{1},
			AggregateConstructors: []func(dt arrow.DataType) Aggregate{
				MakeSum,
			},
		},
	}

	if err := node.Node.Run(execution.Context{Context: ctx}, func(ctx execution.ProduceContext, record execution.Record) error {
		log.Println(record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkGroupBy(b *testing.B) {
	ctx := context.Background()
	allocator := memory.NewGoAllocator()

	rounds := 256
	groups := 10

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	var records []execution.Record
	for arrayIndex := 0; arrayIndex < rounds; arrayIndex++ {
		aBuilder := array.NewInt64Builder(allocator)
		bBuilder := array.NewInt64Builder(allocator)

		for i := 0; i < execution.IdealBatchSize; i++ {
			aBuilder.Append(int64((arrayIndex*execution.IdealBatchSize + i) % groups))
			bBuilder.Append(int64(arrayIndex*execution.IdealBatchSize + i))
		}

		records = append(records, execution.Record{
			Record: array.NewRecord(schema, []arrow.Array{aBuilder.NewArray(), bBuilder.NewArray()}, execution.IdealBatchSize),
		})
	}

	for i := 0; i < b.N; i++ {
		var outArrays []arrow.Array

		b.StopTimer()
		var node execution.NodeWithMeta
		node = execution.NodeWithMeta{
			Node: &TestNode{
				Records: records,
			},
			Schema: schema,
		}
		node = execution.NodeWithMeta{
			Node: &GroupBy{
				OutSchema: arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
						{Name: "b_sum", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					},
					nil,
				),
				Source: node,

				KeyColumns:       []int{0},
				AggregateColumns: []int{1},
				AggregateConstructors: []func(dt arrow.DataType) Aggregate{
					MakeSum,
				},
			},
		}
		b.StartTimer()

		if err := node.Node.Run(execution.Context{Context: ctx}, func(ctx execution.ProduceContext, record execution.Record) error {
			outArrays = append(outArrays, record.Record.Columns()[0])
			return nil
		}); err != nil {
			b.Fatal(err)
		}

		outArrays = outArrays
	}
}

func BenchmarkGroupByString(b *testing.B) {
	ctx := context.Background()
	allocator := memory.NewGoAllocator()

	rounds := 256

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	var records []execution.Record
	for arrayIndex := 0; arrayIndex < rounds; arrayIndex++ {
		aBuilder := array.NewStringBuilder(allocator)

		for i := 0; i < execution.IdealBatchSize; i++ {
			switch rand.Intn(4) {
			case 0:
				aBuilder.Append("aaa")
			case 1:
				aBuilder.Append("bbb")
			case 2:
				aBuilder.Append("ccc")
			case 3:
				aBuilder.Append("ddd")
			}
		}

		records = append(records, execution.Record{
			Record: array.NewRecord(schema, []arrow.Array{aBuilder.NewArray()}, execution.IdealBatchSize),
		})
	}

	for i := 0; i < b.N; i++ {
		var outArrays []arrow.Array

		b.StopTimer()
		var node execution.NodeWithMeta
		node = execution.NodeWithMeta{
			Node: &TestNode{
				Records: records,
			},
			Schema: schema,
		}
		node = execution.NodeWithMeta{
			Node: &GroupBy{
				OutSchema: arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.BinaryTypes.String, Nullable: false},
						{Name: "a_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					},
					nil,
				),
				Source: node,

				KeyColumns:       []int{0},
				AggregateColumns: []int{0},
				AggregateConstructors: []func(dt arrow.DataType) Aggregate{
					MakeCount,
				},
			},
		}
		b.StartTimer()

		if err := node.Node.Run(execution.Context{Context: ctx}, func(ctx execution.ProduceContext, record execution.Record) error {
			outArrays = append(outArrays, record.Record.Columns()[0])
			return nil
		}); err != nil {
			b.Fatal(err)
		}

		outArrays = outArrays
	}
}

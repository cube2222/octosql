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

func TestJoin(t *testing.T) {
	leftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c_0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var leftRecords []execution.Record
	leftRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, leftSchema)
	leftRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	leftRecordBuilder.Field(1).(*array.Int64Builder).Append(2)
	leftRecordBuilder.Field(2).(*array.Int64Builder).Append(3)
	leftRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	leftRecordBuilder.Field(1).(*array.Int64Builder).Append(2)
	leftRecordBuilder.Field(2).(*array.Int64Builder).Append(4)
	leftRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	leftRecordBuilder.Field(1).(*array.Int64Builder).Append(4)
	leftRecordBuilder.Field(2).(*array.Int64Builder).Append(5)
	leftRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	leftRecordBuilder.Field(1).(*array.Int64Builder).Append(5)
	leftRecordBuilder.Field(2).(*array.Int64Builder).Append(5)

	leftRecords = append(leftRecords, execution.Record{
		Record: leftRecordBuilder.NewRecord(),
	})

	rightSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "d_1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var rightRecords []execution.Record
	rightRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, rightSchema)
	rightRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	rightRecordBuilder.Field(1).(*array.Int64Builder).Append(2)
	rightRecordBuilder.Field(2).(*array.Int64Builder).Append(3)
	rightRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	rightRecordBuilder.Field(1).(*array.Int64Builder).Append(2)
	rightRecordBuilder.Field(2).(*array.Int64Builder).Append(5)
	rightRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	rightRecordBuilder.Field(1).(*array.Int64Builder).Append(4)
	rightRecordBuilder.Field(2).(*array.Int64Builder).Append(7)
	rightRecordBuilder.Field(0).(*array.Int64Builder).Append(1)
	rightRecordBuilder.Field(1).(*array.Int64Builder).Append(6)
	rightRecordBuilder.Field(2).(*array.Int64Builder).Append(5)

	rightRecords = append(rightRecords, execution.Record{
		Record: rightRecordBuilder.NewRecord(),
	})

	leftRecordsNode := &execution.NodeWithMeta{
		Node: &TestNode{
			Records:     leftRecords,
			Repetitions: 2,
		},
		Schema: leftSchema,
	}
	rightRecordsNode := &execution.NodeWithMeta{
		Node: &TestNode{
			Records:     rightRecords,
			Repetitions: 2,
		},
		Schema: rightSchema,
	}
	node := &StreamJoin{
		Left:            leftRecordsNode,
		Right:           rightRecordsNode,
		LeftKeyIndices:  []int{0, 1},
		RightKeyIndices: []int{0, 1},
	}

	for i := 0; i < len(rightRecords); i++ {
		if err := node.Run(execution.Context{Context: context.Background()}, func(produceCtx execution.ProduceContext, record execution.Record) error {
			log.Println("output record:", record.Record)
			return nil
		}); err != nil {
			t.Errorf("couldn't run join: %v", err)
		}
	}
}

func BenchmarkJoin_BigTable(b *testing.B) {
	b.StopTimer()
	leftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c_0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var leftRecords []execution.Record
	leftRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, leftSchema)
	leftRecordsCount := 0
	for i := 0; i < 8; i++ {
		for j := 0; j < execution.IdealBatchSize; j++ {
			leftRecordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(1024)))
			leftRecordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(50)))
			leftRecordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(100)))
			leftRecordsCount++
		}

		leftRecords = append(leftRecords, execution.Record{
			Record: leftRecordBuilder.NewRecord(),
		})
	}

	rightSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "d_1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var rightRecords []execution.Record
	rightRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, rightSchema)
	rightRecordsCount := 0
	for i := 0; i < 1600; i++ {
		for j := 0; j < execution.IdealBatchSize; j++ {
			rightRecordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(1024)))
			rightRecordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(50)))
			rightRecordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(1024)))
			rightRecordsCount++
		}

		rightRecords = append(rightRecords, execution.Record{
			Record: rightRecordBuilder.NewRecord(),
		})
	}
	b.StartTimer()

	leftRecordsNode := &execution.NodeWithMeta{
		Node: &TestNode{
			Records: leftRecords,
		},
		Schema: leftSchema,
	}
	rightRecordsNode := &execution.NodeWithMeta{
		Node: &TestNode{
			Records: rightRecords,
		},
		Schema: rightSchema,
	}
	node := &StreamJoin{
		Left:            leftRecordsNode,
		Right:           rightRecordsNode,
		LeftKeyIndices:  []int{0, 1},
		RightKeyIndices: []int{0, 1},
	}

	for benchIndex := 0; benchIndex < b.N; benchIndex++ {
		outputRowCount := 0
		if err := node.Run(execution.Context{Context: context.Background()}, func(produceCtx execution.ProduceContext, record execution.Record) error {
			outputRowCount += int(record.NumRows())
			return nil
		}); err != nil {
			b.Errorf("couldn't run join: %v", err)
		}
		log.Println("left row count:", leftRecordsCount)
		log.Println("right row count:", rightRecordsCount)
		log.Println("output row count:", outputRowCount)
		log.Println()
	}
}

func BenchmarkJoin_SmallTable(b *testing.B) {
	b.StopTimer()
	leftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c_0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var leftRecords []execution.Record
	leftRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, leftSchema)
	leftRecordsCount := 0
	for i := 0; i < 1; i++ {
		for j := 0; j < 1024; j++ {
			leftRecordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(128)))
			leftRecordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(3)))
			leftRecordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(100)))
			leftRecordsCount++
		}

		leftRecords = append(leftRecords, execution.Record{
			Record: leftRecordBuilder.NewRecord(),
		})
	}

	rightSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "d_1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var rightRecords []execution.Record
	rightRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, rightSchema)
	rightRecordsCount := 0
	for i := 0; i < 1800; i++ {
		for j := 0; j < execution.IdealBatchSize; j++ {
			rightRecordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(128)))
			rightRecordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(3)))
			rightRecordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(1024)))
			rightRecordsCount++
		}

		rightRecords = append(rightRecords, execution.Record{
			Record: rightRecordBuilder.NewRecord(),
		})
	}
	b.StartTimer()

	leftRecordsNode := &execution.NodeWithMeta{
		Node: &TestNode{
			Records: leftRecords,
		},
		Schema: leftSchema,
	}
	rightRecordsNode := &execution.NodeWithMeta{
		Node: &TestNode{
			Records: rightRecords,
		},
		Schema: rightSchema,
	}
	node := &StreamJoin{
		Left:            leftRecordsNode,
		Right:           rightRecordsNode,
		LeftKeyIndices:  []int{0, 1},
		RightKeyIndices: []int{0, 1},
	}

	for benchIndex := 0; benchIndex < b.N; benchIndex++ {
		outputRowCount := 0
		if err := node.Run(execution.Context{Context: context.Background()}, func(produceCtx execution.ProduceContext, record execution.Record) error {
			outputRowCount += int(record.NumRows())
			return nil
		}); err != nil {
			b.Errorf("couldn't run join: %v", err)
		}
		log.Println("left row count:", leftRecordsCount)
		log.Println("right row count:", rightRecordsCount)
		log.Println("output row count:", outputRowCount)
		log.Println()
	}
}

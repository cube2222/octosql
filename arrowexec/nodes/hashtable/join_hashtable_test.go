package hashtable

import (
	"log"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cube2222/octosql/arrowexec/execution"
)

func TestPartition(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var records []execution.Record
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			recordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(5)))
			recordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(2)))
			recordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(7)))
		}

		records = append(records, execution.Record{
			Record: recordBuilder.NewRecord(),
		})
	}

	table := BuildJoinTable(records, []int{0, 1}, []int{0, 1}, true)

	log.Println("hashes:", table)
}

func BenchmarkPartitionIntegers(b *testing.B) {
	b.StopTimer()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var records []execution.Record
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i := 0; i < 128; i++ {
		for j := 0; j < execution.IdealBatchSize; j++ {
			recordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(1024 * 8)))
			recordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(4)))
			recordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(7)))
		}

		records = append(records, execution.Record{
			Record: recordBuilder.NewRecord(),
		})
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		table := BuildJoinTable(records, []int{0, 1}, []int{0, 1}, true)
		table = table
	}
}

func TestJoinWithRecord(t *testing.T) {
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

	// execution.IdealBatchSize = 4

	table := BuildJoinTable(leftRecords, []int{0, 1}, []int{0, 1}, true)

	for i := 0; i < len(rightRecords); i++ {
		table.JoinWithRecord(rightRecords[i], func(record execution.Record) {
			log.Println("output record:", record.Record)
		})
	}
}

func TestJoinWithRecord2(t *testing.T) {
	leftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b_0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c_0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var leftRecords []execution.Record
	leftRecordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, leftSchema)
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			leftRecordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(7)))
			leftRecordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(2)))
			leftRecordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(7)))
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
	for i := 0; i < 5; i++ {
		for j := 0; j < 15; j++ {
			rightRecordBuilder.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(7)))
			rightRecordBuilder.Field(1).(*array.Int64Builder).Append(int64(rand.Intn(2)))
			rightRecordBuilder.Field(2).(*array.Int64Builder).Append(int64(rand.Intn(7)))
		}

		rightRecords = append(rightRecords, execution.Record{
			Record: rightRecordBuilder.NewRecord(),
		})
	}

	// execution.IdealBatchSize = 8

	table := BuildJoinTable(leftRecords, []int{0, 1}, []int{0, 1}, false)

	for i := 0; i < len(rightRecords); i++ {
		table.JoinWithRecord(rightRecords[i], func(record execution.Record) {
			log.Println("output record:", record.Record)
		})
	}
}

func BenchmarkJoinTable_JoinWithRecord_BigTable(b *testing.B) {
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
	for i := 0; i < 400; i++ {
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

	for benchIndex := 0; benchIndex < b.N; benchIndex++ {
		table := BuildJoinTable(leftRecords, []int{0, 1}, []int{0, 1}, false)
		outputRowCount := 0
		for i := 0; i < len(rightRecords); i++ {
			table.JoinWithRecord(rightRecords[i], func(record execution.Record) {
				outputRowCount += int(record.NumRows())
			})
		}
		log.Println("left row count:", leftRecordsCount)
		log.Println("right row count:", rightRecordsCount)
		log.Println("output row count:", outputRowCount)
		log.Println()
	}
}

func BenchmarkJoinTable_JoinWithRecord_SmallTable(b *testing.B) {
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
	for i := 0; i < 400; i++ {
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

	for benchIndex := 0; benchIndex < b.N; benchIndex++ {
		table := BuildJoinTable(leftRecords, []int{0, 1}, []int{0, 1}, false)
		outputRowCount := 0
		for i := 0; i < len(rightRecords); i++ {
			table.JoinWithRecord(rightRecords[i], func(record execution.Record) {
				outputRowCount += int(record.NumRows())
			})
		}
		log.Println("left row count:", leftRecordsCount)
		log.Println("right row count:", rightRecordsCount)
		log.Println("output row count:", outputRowCount)
		log.Println()
	}
}

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

	table := BuildJoinTable(records, []int{0, 1})

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
		table := BuildJoinTable(records, []int{0, 1})
		table = table
	}
}

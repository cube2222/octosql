package hashtable

import (
	"runtime"
	"sync"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/brentp/intintmap"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/arrowexec/nodes/helpers"
	"github.com/twotwotwo/sorts"
	"golang.org/x/sync/errgroup"
)

type JoinHashTable struct {
	hashStartIndices *intintmap.Map
	hashes           *array.Uint64
	values           execution.Record
}

// BuildHashTable groups the records by their key hashes, and returns them with an index of partition starts.
func BuildHashTable(records []execution.Record, indices []int) *JoinHashTable {
	// TODO: Handle case where there are 0 records.
	keyHashers := make([]func(rowIndex uint) uint64, len(records))
	for i, record := range records {
		keyHashers[i] = helpers.MakeKeyHasher(record, indices)
	}

	var overallRowCount int
	for _, record := range records {
		overallRowCount += int(record.NumRows())
	}

	hashPositionsOrdered := make([]hashRowPosition, overallRowCount)
	i := 0
	for recordIndex, record := range records {
		numRows := int(record.NumRows())
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			hash := keyHashers[recordIndex](uint(rowIndex))
			hashPositionsOrdered[i] = hashRowPosition{
				hash:        hash,
				recordIndex: recordIndex,
				rowIndex:    rowIndex,
			}
			i++
		}
	}

	sorts.ByUint64(SortHashPosition(hashPositionsOrdered))

	var wg sync.WaitGroup
	wg.Add(2)

	var hashIndex *intintmap.Map
	go func() {
		hashIndex = buildHashIndex(hashPositionsOrdered)
		wg.Done()
	}()

	var hashesArray *array.Uint64
	go func() {
		hashesArray = buildHashesArray(overallRowCount, hashPositionsOrdered)
		wg.Done()
	}()

	record := buildRecords(records, overallRowCount, hashPositionsOrdered)

	wg.Wait()

	return &JoinHashTable{
		hashStartIndices: hashIndex,
		hashes:           hashesArray,
		values:           execution.Record{Record: record},
	}
}

func buildHashIndex(hashPositionsOrdered []hashRowPosition) *intintmap.Map {
	hashIndex := intintmap.New(1024, 0.6)
	hashIndex.Put(int64(hashPositionsOrdered[0].hash), 0)
	for i := 1; i < len(hashPositionsOrdered); i++ {
		if hashPositionsOrdered[i].hash != hashPositionsOrdered[i-1].hash {
			hashIndex.Put(int64(hashPositionsOrdered[i].hash), int64(i))
		}
	}
	return hashIndex
}

type hashRowPosition struct {
	hash        uint64
	recordIndex int
	rowIndex    int
}

func buildHashesArray(overallRowCount int, hashPositionsOrdered []hashRowPosition) *array.Uint64 {
	hashesBuilder := array.NewUint64Builder(memory.NewGoAllocator()) // TODO: Get allocator from argument.
	hashesBuilder.Reserve(overallRowCount)
	for _, hashPosition := range hashPositionsOrdered {
		hashesBuilder.UnsafeAppend(hashPosition.hash)
	}
	return hashesBuilder.NewUint64Array()
}

func buildRecords(records []execution.Record, overallRowCount int, hashPositionsOrdered []hashRowPosition) arrow.Record {
	// TODO: Get allocator from argument.
	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), records[0].Schema())
	recordBuilder.Reserve(overallRowCount)

	var g errgroup.Group
	g.SetLimit(runtime.GOMAXPROCS(0))

	columnCount := len(recordBuilder.Fields())
	for columnIndex := 0; columnIndex < columnCount; columnIndex++ {
		columnRewriters := make([]func(rowIndex int), len(records))
		for recordIndex, record := range records {
			columnRewriters[recordIndex] = helpers.MakeColumnRewriter(recordBuilder.Field(columnIndex), record.Column(columnIndex))
		}

		g.Go(func() error {
			for _, hashPosition := range hashPositionsOrdered {
				columnRewriters[hashPosition.recordIndex](hashPosition.rowIndex)
			}
			return nil
		})
	}
	g.Wait()
	record := recordBuilder.NewRecord()
	return record
}

type SortHashPosition []hashRowPosition

func (h SortHashPosition) Len() int {
	return len(h)
}

func (h SortHashPosition) Less(i, j int) bool {
	return h[i].hash < h[j].hash
}

func (h SortHashPosition) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h SortHashPosition) Key(i int) uint64 {
	return h[i].hash
}

package hashtable

import (
	"runtime"
	"sync"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/brentp/intintmap"
	"github.com/cube2222/octosql/arrowexec/execution"
	helpers2 "github.com/cube2222/octosql/arrowexec/helpers"
	"github.com/twotwotwo/sorts"
	"golang.org/x/sync/errgroup"
)

type JoinTable struct {
	partitions []JoinTablePartition

	keyIndices, joinedKeyIndices []int
	tableIsLeftSide              bool
}

type JoinTablePartition struct {
	hashStartIndices *intintmap.Map
	hashes           *array.Uint64
	values           execution.Record
}

func BuildJoinTable(records []execution.Record, keyIndices, joinedKeyIndices []int, tableIsLeftSide bool) *JoinTable {
	if len(keyIndices) != len(joinedKeyIndices) {
		panic("table key and joined key indices don't have the same length")
	}

	partitions := buildJoinTablePartitions(records, keyIndices)
	return &JoinTable{
		partitions:       partitions,
		keyIndices:       keyIndices,
		joinedKeyIndices: joinedKeyIndices,
		tableIsLeftSide:  tableIsLeftSide,
	}
}

func buildJoinTablePartitions(records []execution.Record, keyIndices []int) []JoinTablePartition {
	partitions := 7 // TODO: Make it the first prime number larger than the core count.

	// TODO: Handle case where there are 0 records.
	keyHashers := make([]func(rowIndex uint) uint64, len(records))
	for i, record := range records {
		keyHashers[i] = helpers2.MakeKeyHasher(record, keyIndices)
	}

	var overallRowCount int
	for _, record := range records {
		overallRowCount += int(record.NumRows())
	}

	hashPositionsOrdered := make([][]hashRowPosition, partitions)
	for i := range hashPositionsOrdered {
		hashPositionsOrdered[i] = make([]hashRowPosition, 0, overallRowCount/partitions)
	}

	for recordIndex, record := range records {
		numRows := int(record.NumRows())
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			hash := keyHashers[recordIndex](uint(rowIndex))
			partition := int(hash % uint64(partitions))
			hashPositionsOrdered[partition] = append(hashPositionsOrdered[partition], hashRowPosition{
				hash:        hash,
				recordIndex: recordIndex,
				rowIndex:    rowIndex,
			})
		}
	}

	var wg sync.WaitGroup
	wg.Add(partitions)
	joinTablePartitions := make([]JoinTablePartition, partitions)
	for part := 0; part < partitions; part++ {
		part := part

		go func() {
			hashPositionsOrderedPartition := hashPositionsOrdered[part]
			sorts.ByUint64(SortHashPosition(hashPositionsOrderedPartition))

			hashIndex := buildHashIndex(hashPositionsOrderedPartition)
			hashesArray := buildHashesArray(hashPositionsOrderedPartition)
			record := buildRecords(records, hashPositionsOrderedPartition)

			joinTablePartitions[part] = JoinTablePartition{
				hashStartIndices: hashIndex,
				hashes:           hashesArray,
				values:           execution.Record{Record: record},
			}
			wg.Done()
		}()
	}
	wg.Wait()

	return joinTablePartitions
}

func buildHashIndex(hashPositionsOrdered []hashRowPosition) *intintmap.Map {
	if len(hashPositionsOrdered) == 0 {
		return intintmap.New(1, 0.6)
	}
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

func buildHashesArray(hashPositionsOrdered []hashRowPosition) *array.Uint64 {
	hashesBuilder := array.NewUint64Builder(memory.NewGoAllocator()) // TODO: Get allocator from argument.
	hashesBuilder.Reserve(len(hashPositionsOrdered))
	for _, hashPosition := range hashPositionsOrdered {
		hashesBuilder.UnsafeAppend(hashPosition.hash)
	}
	return hashesBuilder.NewUint64Array()
}

func buildRecords(records []execution.Record, hashPositionsOrdered []hashRowPosition) arrow.Record {
	// TODO: Get allocator from argument.
	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), records[0].Schema())
	recordBuilder.Reserve(len(hashPositionsOrdered))

	var g errgroup.Group
	g.SetLimit(runtime.GOMAXPROCS(0))

	columnCount := len(recordBuilder.Fields())
	for columnIndex := 0; columnIndex < columnCount; columnIndex++ {
		columnRewriters := make([]func(rowIndex int), len(records))
		for recordIndex, record := range records {
			columnRewriters[recordIndex] = helpers2.MakeColumnRewriter(recordBuilder.Field(columnIndex), record.Column(columnIndex))
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

func (t *JoinTable) JoinWithRecord(record execution.Record, produce func(execution.Record)) {
	var outFields []arrow.Field
	if t.tableIsLeftSide {
		outFields = append(outFields, t.partitions[0].values.Schema().Fields()...)
		outFields = append(outFields, record.Schema().Fields()...)
	} else {
		outFields = append(outFields, record.Schema().Fields()...)
		outFields = append(outFields, t.partitions[0].values.Schema().Fields()...)
	}
	outSchema := arrow.NewSchema(outFields, nil)

	recordKeyHasher := helpers2.MakeKeyHasher(record, t.joinedKeyIndices)

	partitionKeyEqualityCheckers := make([]func(joinedRowIndex int, tableRowIndex int) bool, len(t.partitions))
	for partitionIndex := range t.partitions {
		partitionKeyEqualityCheckers[partitionIndex] = helpers2.MakeKeyEqualityChecker(record, t.partitions[partitionIndex].values, t.keyIndices, t.joinedKeyIndices)
	}

	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), outSchema)
	curOutRecordRowCount := 0

	partitionColumnRewriters := make([]func(joinedRowIndex int, tableRowIndex int), len(t.partitions))
	for partitionIndex := range t.partitions {
		partitionColumnRewriters[partitionIndex] = t.makeRecordRewriterForPartition(record, recordBuilder, partitionIndex)
	}

	numRows := int(record.NumRows())
	for recordRowIndex := 0; recordRowIndex < numRows; recordRowIndex++ {
		keyHash := recordKeyHasher(uint(recordRowIndex))

		partitionIndex := int(keyHash % uint64(len(t.partitions)))
		partition := t.partitions[partitionIndex]

		firstMatchingHashIndex, ok := partition.hashStartIndices.Get(int64(keyHash))
		if !ok {
			continue
		}

		for tableRowIndex := int(firstMatchingHashIndex); tableRowIndex < partition.hashes.Len(); tableRowIndex++ {
			if partition.hashes.Value(tableRowIndex) != keyHash {
				break
			}
			if partitionKeyEqualityCheckers[partitionIndex](recordRowIndex, tableRowIndex) {
				partitionColumnRewriters[partitionIndex](recordRowIndex, tableRowIndex)
				curOutRecordRowCount++

				if curOutRecordRowCount >= execution.IdealBatchSize {
					produce(execution.Record{Record: recordBuilder.NewRecord()})
					curOutRecordRowCount = 0
				}
			}
		}
	}
	if curOutRecordRowCount > 0 {
		produce(execution.Record{Record: recordBuilder.NewRecord()})
	}
	return
}

func (t *JoinTable) makeRecordRewriterForPartition(joinedRecord execution.Record, recordBuilder *array.RecordBuilder, partitionIndex int) func(joinedRowIndex int, tableRowIndex int) {
	partition := t.partitions[partitionIndex]

	var joinedRecordColumnOffset, tableColumnOffset int
	if t.tableIsLeftSide {
		tableColumnOffset = 0
		joinedRecordColumnOffset = len(partition.values.Columns())
	} else {
		joinedRecordColumnOffset = 0
		tableColumnOffset = len(joinedRecord.Columns())
	}

	joinedRecordColumnRewriters := make([]func(rowIndex int), len(joinedRecord.Columns()))
	for columnIndex := range joinedRecord.Columns() {
		joinedRecordColumnRewriters[columnIndex] = helpers2.MakeColumnRewriter(recordBuilder.Field(joinedRecordColumnOffset+columnIndex), joinedRecord.Column(columnIndex))
	}

	tableColumnRewriters := make([]func(rowIndex int), len(partition.values.Columns()))
	for columnIndex := range partition.values.Columns() {
		tableColumnRewriters[columnIndex] = helpers2.MakeColumnRewriter(recordBuilder.Field(tableColumnOffset+columnIndex), partition.values.Column(columnIndex))
	}

	return func(joinedRowIndex int, tableRowIndex int) {
		for _, columnRewriter := range joinedRecordColumnRewriters {
			columnRewriter(joinedRowIndex)
		}
		for _, columnRewriter := range tableColumnRewriters {
			columnRewriter(tableRowIndex)
		}
	}
}

package nodes

import (
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/brentp/intintmap"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/arrowexec/nodes/helpers"
)

type GroupBy struct {
	OutSchema *arrow.Schema
	Source    execution.NodeWithMeta

	// Both keys and aggregate columns have to be calculated by a preceding map.

	KeyColumns            []int
	AggregateConstructors []func(dt arrow.DataType) Aggregate
	AggregateColumns      []int
}

func (g *GroupBy) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	entryCount := 0
	entryIndices := intintmap.New(16, 0.6)
	aggregates := make([]Aggregate, len(g.AggregateConstructors))
	for i := range aggregates {
		aggregates[i] = g.AggregateConstructors[i](g.Source.Schema.Field(g.AggregateColumns[i]).Type)
	}
	key := make([]Key, len(g.KeyColumns))
	for i := range key {
		key[i] = MakeKey(g.Source.Schema.Field(g.KeyColumns[i]).Type)
	}

	if err := g.Source.Node.Run(ctx, func(ctx execution.ProduceContext, record execution.Record) error {
		getKeyHash := helpers.MakeKeyHasher(record, g.KeyColumns)

		aggColumnConsumers := make([]func(entryIndex uint, rowIndex uint), len(aggregates))
		for i := range aggColumnConsumers {
			aggColumnConsumers[i] = aggregates[i].MakeColumnConsumer(record.Column(g.AggregateColumns[i]))
		}
		newKeyAdders := make([]func(rowIndex uint), len(key))
		for i := range newKeyAdders {
			newKeyAdders[i] = key[i].MakeNewKeyAdder(record.Column(g.KeyColumns[i]))
		}
		keyEqualityCheckers := make([]func(entryIndex uint, rowIndex uint) bool, len(key))
		for i := range keyEqualityCheckers {
			keyEqualityCheckers[i] = key[i].MakeKeyEqualityChecker(record.Column(g.KeyColumns[i]))
		}

		rows := record.NumRows()
		for rowIndex := uint(0); rowIndex < uint(rows); rowIndex++ {
			hash := getKeyHash(rowIndex)
			entryIndex, ok := entryIndices.Get(int64(hash))
			if !ok {
				entryIndex = int64(entryCount)
				entryCount++
				entryIndices.Put(int64(hash), entryIndex)
				for _, addKey := range newKeyAdders {
					addKey(rowIndex)
				}
			} else {
				// ok, double-check equality
				equal := true
				for _, checkKey := range keyEqualityCheckers {
					equal = equal && checkKey(uint(entryIndex), rowIndex)
					if !equal {
						break
					}
				}
				if !equal {
					// TODO: Really, we should instead use an int -> []int map here, and if there's no equality, we should append a new index to the slice.
					panic("hash collision")
				}
			}

			for _, consume := range aggColumnConsumers {
				consume(uint(entryIndex), rowIndex)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	for batchIndex := 0; batchIndex < (entryCount/execution.IdealBatchSize)+1; batchIndex++ {
		offset := batchIndex * execution.IdealBatchSize
		length := entryCount - offset
		if length > execution.IdealBatchSize {
			length = execution.IdealBatchSize
		}

		columns := make([]arrow.Array, len(g.OutSchema.Fields()))
		for i := range key {
			columns[i] = key[i].GetBatch(length, offset)
		}
		for i := range aggregates {
			columns[len(key)+i] = aggregates[i].GetBatch(length, offset)
		}

		record := array.NewRecord(g.OutSchema, columns, int64(length))

		if err := produce(execution.ProduceContext{Context: ctx}, execution.Record{Record: record}); err != nil {
			return err
		}
	}

	return nil
}

type Aggregate interface {
	MakeColumnConsumer(array arrow.Array) func(entryIndex uint, rowIndex uint)
	GetBatch(length int, offset int) arrow.Array
}

func MakeCount(dt arrow.DataType) Aggregate {
	return &Count{
		data: memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
	}
}

type Count struct {
	data  *memory.Buffer
	state []int64 // This uses the above data as the storage underneath.
}

func (agg *Count) MakeColumnConsumer(arr arrow.Array) func(entryIndex uint, rowIndex uint) {
	return func(entryIndex uint, rowIndex uint) {
		if entryIndex >= uint(len(agg.state)) {
			agg.data.Resize(arrow.Int64Traits.BytesRequired(bitutil.NextPowerOf2(int(entryIndex) + 1)))
			agg.state = arrow.Int64Traits.CastFromBytes(agg.data.Bytes())
		}
		agg.state[entryIndex]++
	}
}

func (agg *Count) GetBatch(length int, offset int) arrow.Array {
	return array.NewInt64Data(array.NewData(arrow.PrimitiveTypes.Int64, length, []*memory.Buffer{nil, agg.data}, nil, 0 /*TODO: Fixme*/, offset))
}

func MakeSum(dt arrow.DataType) Aggregate { // TODO: octosql.Type?
	switch dt.ID() {
	case arrow.INT64:
		return &SumInt{
			data: memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
		}
	default:
		panic("unsupported type for sum")
	}
	// TODO: Implement for nullable, probably a wrapping aggregate column consumer that just ignores nulls. Actually, that wrapper would set the null bitmap.
}

type SumInt struct {
	data  *memory.Buffer
	state []int64 // This uses the above data as the storage underneath.
}

func (agg *SumInt) MakeColumnConsumer(arr arrow.Array) func(entryIndex uint, rowIndex uint) {
	typedArr := arr.(*array.Int64).Int64Values()
	return func(entryIndex uint, rowIndex uint) {
		if entryIndex >= uint(len(agg.state)) {
			agg.data.Resize(arrow.Int64Traits.BytesRequired(bitutil.NextPowerOf2(int(entryIndex) + 1)))
			agg.state = arrow.Int64Traits.CastFromBytes(agg.data.Bytes())
		}
		agg.state[entryIndex] += typedArr[rowIndex]
	}
}

func (agg *SumInt) GetBatch(length int, offset int) arrow.Array {
	return array.NewInt64Data(array.NewData(arrow.PrimitiveTypes.Int64, length, []*memory.Buffer{nil, agg.data}, nil, 0 /*TODO: Fixme*/, offset))
}

func MakeKey(dt arrow.DataType) Key {
	switch dt.ID() {
	case arrow.INT64:
		return &KeyInt{
			data: memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
		}
	case arrow.STRING:
		return &KeyString{
			stringData:  memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
			offsetsData: memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
		}
	default:
		panic("unsupported type for group by key")
	}
}

type Key interface {
	MakeNewKeyAdder(array arrow.Array) func(rowIndex uint)
	MakeKeyEqualityChecker(arr arrow.Array) func(entryIndex uint, rowIndex uint) bool
	GetBatch(length int, offset int) arrow.Array
}

type KeyInt struct {
	data  *memory.Buffer // TODO: Release these buffers at some point.
	state []int64        // This uses the above data as the storage underneath.
	count int
}

func (key *KeyInt) MakeNewKeyAdder(arr arrow.Array) func(rowIndex uint) {
	typedArr := arr.(*array.Int64).Int64Values()
	return func(rowIndex uint) {
		if key.count >= len(key.state) {
			key.data.Resize(arrow.Int64Traits.BytesRequired(bitutil.NextPowerOf2(key.count + 1)))
			key.state = arrow.Int64Traits.CastFromBytes(key.data.Bytes())
		}
		key.state[key.count] = typedArr[rowIndex]
		key.count++
	}
}

func (key *KeyInt) MakeKeyEqualityChecker(arr arrow.Array) func(entryIndex uint, rowIndex uint) bool {
	typedArr := arr.(*array.Int64).Int64Values()
	return func(entryIndex uint, rowIndex uint) bool {
		return typedArr[rowIndex] == key.state[entryIndex]
	}
}

func (key *KeyInt) GetBatch(length int, offset int) arrow.Array {
	return array.NewInt64Data(array.NewData(arrow.PrimitiveTypes.Int64, length, []*memory.Buffer{nil, key.data}, nil, 0 /*TODO: Fixme*/, offset))
}

type KeyString struct {
	// TODO: Use a LargeStringArray, instead of a StringArray.
	// We use a custom set of buffers, rather than using the builder, because accessing elements of the builder is slow.
	offsetsData     *memory.Buffer // TODO: Release these buffers at some point.
	offsetsState    []int32        // This uses the above data as the storage underneath.
	stringData      *memory.Buffer // TODO: Release these buffers at some point.
	stringByteState []byte         // This uses the above data as the storage underneath.
	stringState     string

	length int
	count  int

	finishedArray *array.String
}

func (key *KeyString) MakeNewKeyAdder(arr arrow.Array) func(rowIndex uint) {
	typedArr := arr.(*array.String)
	return func(rowIndex uint) {
		value := typedArr.Value(int(rowIndex))
		if key.length+len(value) > len(key.stringByteState) {
			key.stringData.Resize(bitutil.NextPowerOf2(key.length + len(value)))
			key.stringByteState = key.stringData.Bytes()
			key.stringState = *(*string)(unsafe.Pointer(&key.stringByteState))
		}
		// We always want a spare place for the final offset, so we add 1 for that.
		potentialFinalOffsetCount := key.count + 2
		if potentialFinalOffsetCount > len(key.offsetsState) {
			key.offsetsData.Resize(arrow.Int32Traits.BytesRequired(bitutil.NextPowerOf2(potentialFinalOffsetCount)))
			key.offsetsState = arrow.Int32Traits.CastFromBytes(key.offsetsData.Bytes())
		}

		key.offsetsState[key.count] = int32(key.length)
		copy(key.stringByteState[key.length:], value)

		key.length += len(value)
		key.count++
	}
}

func (key *KeyString) MakeKeyEqualityChecker(arr arrow.Array) func(entryIndex uint, rowIndex uint) bool {
	typedArr := arr.(*array.String)
	return func(entryIndex uint, rowIndex uint) bool {
		start := key.offsetsState[entryIndex]
		end := key.length
		if int(entryIndex) < key.count-1 {
			end = int(key.offsetsState[entryIndex+1])
		}
		return typedArr.Value(int(rowIndex)) == key.stringState[start:end]
	}
}

func (key *KeyString) GetBatch(length int, offset int) arrow.Array {
	if key.finishedArray == nil {
		// We reserved an extra spot when resizing, so we can just add the final offset.
		key.offsetsState[key.count] = int32(key.length)
		key.finishedArray = array.NewStringData(array.NewData(arrow.BinaryTypes.String, key.count, []*memory.Buffer{nil, key.offsetsData, key.stringData}, nil, 0 /*TODO: Fixme*/, 0))
	}

	return array.NewSlice(key.finishedArray, int64(offset), int64(offset+length))
}

package nodes

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/brentp/intintmap"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/segmentio/fasthash/fnv1a"
)

// TODO: Separate implementation for variable-size binary key and fixed-size binary key?
// TODO: For now we just implement hash value equality, but it really should store a list of indices underneath and compare for actual value equality.

type GroupBy struct {
	OutSchema *arrow.Schema
	Source    execution.NodeWithMeta

	KeyExprs              []int // For now, let's just use indices here.
	AggregateConstructors []func(dt arrow.DataType) Aggregate
	AggregateExprs        []int // For now, let's just use indices here.
}

func (g *GroupBy) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	entryCount := 0
	entryIndices := intintmap.New(16, 0.6)
	// entryIndices := make(map[uint64]uint)
	aggregates := make([]Aggregate, len(g.AggregateConstructors))
	for i := range aggregates {
		aggregates[i] = g.AggregateConstructors[i](g.Source.Schema.Field(g.AggregateExprs[i]).Type)
	}
	key := make([]Key, len(g.KeyExprs))
	for i := range key {
		key[i] = MakeKey(g.Source.Schema.Field(g.KeyExprs[i]).Type)
	}

	if err := g.Source.Node.Run(ctx, func(ctx execution.ProduceContext, record execution.Record) error {
		getKeyHash := MakeKeyHasher(g.Source.Schema, record, g.KeyExprs)

		aggColumnConsumers := make([]func(entryIndex uint, rowIndex uint), len(aggregates))
		for i := range aggColumnConsumers {
			aggColumnConsumers[i] = aggregates[i].MakeColumnConsumer(record.Column(g.AggregateExprs[i]))
		}
		newKeyAdders := make([]func(rowIndex uint), len(key))
		for i := range newKeyAdders {
			newKeyAdders[i] = key[i].MakeNewKeyAdder(record.Column(g.KeyExprs[i]))
		}
		keyEqualityCheckers := make([]func(entryIndex uint, rowIndex uint) bool, len(key))
		for i := range keyEqualityCheckers {
			keyEqualityCheckers[i] = key[i].MakeKeyEqualityChecker(record.Column(g.KeyExprs[i]))
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

	for batchIndex := 0; batchIndex < (entryCount/execution.BatchSize)+1; batchIndex++ {
		offset := batchIndex * execution.BatchSize
		length := entryCount - offset
		if length > execution.BatchSize {
			length = execution.BatchSize
		}

		columns := make([]arrow.Array, len(g.OutSchema.Fields()))
		for i := range key {
			columns[i] = key[i].GetBatch(length, offset)
		}
		for i := range aggregates {
			columns[len(key)+i] = aggregates[i].GetBatch(length, offset)
		}

		record := array.NewRecord(g.OutSchema, columns, int64(length))

		if err := produce(ctx, execution.Record{Record: record}); err != nil {
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

func MakeKeyHasher(schema *arrow.Schema, record execution.Record, keyIndices []int) func(rowIndex uint) uint64 {
	subHashers := make([]func(hash uint64, rowIndex uint) uint64, len(keyIndices))
	for i, colIndex := range keyIndices {
		switch schema.Field(colIndex).Type.ID() {
		case arrow.INT64:
			typedArr := record.Column(colIndex).(*array.Int64).Int64Values()
			subHashers[i] = func(hash uint64, rowIndex uint) uint64 {
				return fnv1a.AddUint64(hash, uint64(typedArr[rowIndex]))
			}
		case arrow.STRING:
			typedArr := record.Column(colIndex).(*array.String)
			subHashers[i] = func(hash uint64, rowIndex uint) uint64 {
				return fnv1a.AddString64(hash, typedArr.Value(int(rowIndex)))
			}
		default:
			panic("unsupported")
		}
	}
	return func(rowIndex uint) uint64 {
		hash := fnv1a.Init64
		for _, hasher := range subHashers {
			hash = hasher(hash, rowIndex)
		}
		return hash
	}
}

func MakeKey(dt arrow.DataType) Key {
	switch dt.ID() {
	case arrow.INT64:
		return &KeyInt{
			data: memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
		}
	case arrow.STRING:
		return &KeyString{
			builder: array.NewStringBuilder(memory.NewGoAllocator()), // TODO: Get allocator as argument.
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
	builder *array.StringBuilder

	finishedArray *array.String
}

func (key *KeyString) MakeNewKeyAdder(arr arrow.Array) func(rowIndex uint) {
	typedArr := arr.(*array.String)
	return func(rowIndex uint) {
		key.builder.Append(typedArr.Value(int(rowIndex)))
		// TODO: Benchmark vs using the raw bytes and offsets underneath.
		//       Just benchmark allocations here.
	}
}

func (key *KeyString) MakeKeyEqualityChecker(arr arrow.Array) func(entryIndex uint, rowIndex uint) bool {
	typedArr := arr.(*array.String)
	return func(entryIndex uint, rowIndex uint) bool {
		return typedArr.Value(int(rowIndex)) == key.builder.Value(int(entryIndex))
	}
}

func (key *KeyString) GetBatch(length int, offset int) arrow.Array {
	if key.finishedArray == nil {
		key.finishedArray = key.builder.NewStringArray()
	}
	// TODO: Easier - return the whole array, then use array.NewSlice to slice it into batches.
	return array.NewSlice(key.finishedArray, int64(offset), int64(offset+length))
}

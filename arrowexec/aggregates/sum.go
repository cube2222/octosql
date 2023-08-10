package aggregates

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cube2222/octosql/arrowexec/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var SumOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Int,
		OutputType:   octosql.Int,
		Prototype:    NewSumIntPrototype(),
	},
}

func NewSumIntPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &SumInt{
			data: memory.NewResizableBuffer(memory.NewGoAllocator()),
		}
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

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

var CountOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Any,
		OutputType:   octosql.Int,
		Prototype:    NewCountPrototype(),
	},
}

func NewCountPrototype() func() nodes.Aggregate { // TODO: Accept context to prototype.
	return func() nodes.Aggregate {
		return &Count{
			data: memory.NewResizableBuffer(memory.NewGoAllocator()), // TODO: Get allocator as argument.
		}
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

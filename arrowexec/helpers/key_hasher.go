package helpers

import (
	"math"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/segmentio/fasthash/fnv1a"
)

func MakeRecordKeyHasher(record execution.Record, keyIndices []int) func(rowIndex uint) uint64 {
	columns := make([]arrow.Array, len(keyIndices))
	for i := range columns {
		columns[i] = record.Column(keyIndices[i])
	}
	return MakeRowHasher(columns)
}

func MakeRowHasher(columns []arrow.Array) func(rowIndex uint) uint64 {
	subHashers := make([]func(hash uint64, rowIndex uint) uint64, len(columns))
	for i := range columns {
		switch columns[i].DataType().ID() {
		case arrow.INT64:
			typedArr := columns[i].(*array.Int64).Int64Values()
			subHashers[i] = func(hash uint64, rowIndex uint) uint64 {
				return fnv1a.AddUint64(hash, uint64(typedArr[rowIndex]))
			}
		case arrow.FLOAT64:
			typedArr := columns[i].(*array.Float64).Float64Values()
			subHashers[i] = func(hash uint64, rowIndex uint) uint64 {
				return fnv1a.AddUint64(hash, math.Float64bits(typedArr[rowIndex]))
			}
		case arrow.STRING:
			typedArr := columns[i].(*array.String)
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

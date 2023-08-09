package helpers

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/segmentio/fasthash/fnv1a"
)

func MakeKeyHasher(record execution.Record, keyIndices []int) func(rowIndex uint) uint64 {
	subHashers := make([]func(hash uint64, rowIndex uint) uint64, len(keyIndices))
	for i, colIndex := range keyIndices {
		switch record.Column(colIndex).DataType().ID() {
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

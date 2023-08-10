package helpers

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

func MakeColumnRewriter(builder array.Builder, arr arrow.Array) func(rowIndex int) {
	// TODO: Should this operate on row ranges instead of single rows? Would make low-selectivity workloads faster, as well as nested types.
	switch builder.Type().ID() {
	case arrow.INT64:
		typedBuilder := builder.(*array.Int64Builder)
		typedArr := arr.(*array.Int64)
		return func(rowIndex int) {
			typedBuilder.Append(typedArr.Value(rowIndex))
		}
	case arrow.FLOAT64:
		typedBuilder := builder.(*array.Float64Builder)
		typedArr := arr.(*array.Float64)
		return func(rowIndex int) {
			typedBuilder.Append(typedArr.Value(rowIndex))
		}
	// TODO: Add more types.
	default:
		panic(fmt.Errorf("unsupported type for filtering: %v", builder.Type().ID()))
	}
}

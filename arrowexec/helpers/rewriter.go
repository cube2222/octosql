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
		return rewriterForType[int64](builder.(*array.Int64Builder), arr.(*array.Int64))
	case arrow.FLOAT64:
		return rewriterForType[float64](builder.(*array.Float64Builder), arr.(*array.Float64))
	case arrow.STRING:
		return rewriterForType[string](builder.(*array.StringBuilder), arr.(*array.String))
	// TODO: Add more types.
	default:
		panic(fmt.Errorf("unsupported type for rewriting: %v", builder.Type().ID()))
	}
}

func rewriterForType[T any, BuilderType interface{ Append(v T) }, ArrayType interface{ Value(i int) T }](builder BuilderType, arr ArrayType) func(rowIndex int) {
	return func(rowIndex int) {
		builder.Append(arr.Value(rowIndex))
	}
}

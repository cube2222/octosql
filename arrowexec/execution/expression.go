package execution

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/arrow/scalar"
)

type ScalarExpression interface {
	EvaluateScalar(ctx Context) (scalar.Scalar, error)
}

type Expression interface {
	Evaluate(ctx Context, record Record) (arrow.Array, error)
}

type RecordVariable struct {
	index int
}

func NewRecordVariable(index int) *RecordVariable {
	return &RecordVariable{
		index: index,
	}
}

func (r *RecordVariable) Evaluate(ctx Context, record Record) (arrow.Array, error) {
	// TODO: Retain array?
	return record.Column(r.index), nil
}

// TODO: Add ConstArray expression for testing.

type ConstArray struct {
	Array arrow.Array
}

func (c *ConstArray) Evaluate(ctx Context, record Record) (arrow.Array, error) {
	if c.Array.Len() != int(record.NumRows()) {
		panic("const array length doesn't match record length")
	}
	return c.Array, nil
}

// type ParentScopeVariable struct {
//
// }

type FunctionCall struct {
	function         func([]arrow.Array) (arrow.Array, error)
	args             []Expression
	nullCheckIndices []int
	strict           bool
}

func NewFunctionCall(function func([]arrow.Array) (arrow.Array, error), args []Expression, nullCheckIndices []int, strict bool) *FunctionCall {
	return &FunctionCall{
		function:         function,
		args:             args,
		nullCheckIndices: nullCheckIndices,
		strict:           strict,
	}
}

func (f *FunctionCall) Evaluate(ctx Context, record Record) (arrow.Array, error) {
	args := make([]arrow.Array, len(f.args))
	for i, arg := range f.args {
		arr, err := arg.Evaluate(ctx, record)
		if err != nil {
			return nil, fmt.Errorf("couldn't evaluate argument %d: %w", i, err)
		}
		args[i] = arr
	}

	if f.strict {
		// TODO: Handle validity bitmaps.
	}

	return f.function(args)
}

// type ArrowComputeFunctionCall struct {
// 	Name      string
// 	Arguments []Expression
// }
//
// func (f *ArrowComputeFunctionCall) Evaluate(ctx Context, record Record) (arrow.Array, error) {
// 	// TODO: Optimize all of this if it's too slow.
// 	args := make([]compute.Datum, len(f.Arguments))
// 	for i, arg := range f.Arguments {
// 		arr, err := arg.Evaluate(ctx, record)
// 		if err != nil {
// 			return nil, fmt.Errorf("couldn't evaluate argument %d: %w", i, err)
// 		}
// 		args[i] = &compute.ArrayDatum{Value: arr.Data()}
// 	}
//
// 	fn, ok := compute.GetFunctionRegistry().GetFunction(f.Name)
// 	if !ok {
// 		panic("Bug: array function not found")
// 	}
// 	out, err := fn.Execute(ctx.Context, nil, args...)
// 	if err != nil {
// 		return nil, fmt.Errorf("couldn't execute function: %w", err)
// 	}
// 	return array.MakeFromData(out.(*compute.ArrayDatum).Value), nil
// }

type Constant struct {
	Value scalar.Scalar
}

func (c *Constant) Evaluate(ctx Context, record Record) (arrow.Array, error) {
	// TODO: Cache this for the IdealBatchSize.
	return scalar.MakeArrayFromScalar(c.Value, int(record.NumRows()), memory.NewGoAllocator()) // TODO: Add allocator to execution context.
}

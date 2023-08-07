package execution

import (
	"github.com/apache/arrow/go/v13/arrow"
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

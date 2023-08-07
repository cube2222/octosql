package execution

import (
	"context"

	"github.com/apache/arrow/go/v13/arrow"
)

// All nodes will try to create batches of approximately this size. Different sizes are allowed.
const IdealBatchSize = 16 * 1024

type Context struct {
	Context context.Context
	// TODO: We'll also need the variable context here.
	//       Maybe instead of storing a linked list of lists here, we should store references and indices of the underlying arrays?
	//       Basically, store a reference to each parent scope, the relevant record, and entry index of the value in that record.
}
type ProduceContext struct {
	Context
}

type Node interface {
	Run(ctx Context, produce ProduceFunc) error
}

type NodeWithMeta struct {
	Node   Node
	Schema *arrow.Schema
}

type ProduceFunc func(produceCtx ProduceContext, record Record) error

type Record struct {
	arrow.Record
}

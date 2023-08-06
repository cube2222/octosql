package execution

import (
	"context"

	"github.com/apache/arrow/go/v13/arrow"
)

const BatchSize = 16 * 1024

type Context context.Context
type ProduceContext context.Context

type Node interface {
	Run(ctx Context, produce ProduceFunc) error
}

type NodeWithMeta struct {
	Node   Node
	Schema *arrow.Schema
}

type ProduceFunc func(ctx ProduceContext, record Record) error

type Record struct {
	arrow.Record
}

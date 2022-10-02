package eager

import (
	"bufio"
	"io"
	"os"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Format interface {
	SetSchema(physical.Schema)
	Write(values [][]octosql.Value, count int) error
	Close() error
}

type OutputPrinter struct {
	source Node

	schema physical.Schema
	format func(io.Writer) Format
}

func NewOutputPrinter(source Node, schema physical.Schema, format func(io.Writer) Format) *OutputPrinter {
	return &OutputPrinter{
		source: source,
		schema: schema,
		format: format,
	}
}

func (o *OutputPrinter) Run(execCtx ExecutionContext) error {
	w := bufio.NewWriterSize(os.Stdout, 4096*1024)
	format := o.format(w)
	format.SetSchema(o.schema)

	if err := o.source.Run(
		execCtx,
		func(ctx ProduceContext, record RecordBatch) error {
			return format.Write(record.Values, record.Size)
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			return nil
		},
	); err != nil {
		return err
	}

	return w.Flush()
}

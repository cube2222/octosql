package stream

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type Format interface {
	SetSchema(physical.Schema)
	WriteRecord(RecordBatch) error
	WriteMeta(MetadataMessage) error
	Close() error
}

type OutputPrinter struct {
	source Node
	format Format
}

func NewOutputPrinter(source Node, format Format) *OutputPrinter {
	return &OutputPrinter{
		source: source,
		format: format,
	}
}

func (o *OutputPrinter) Run(execCtx ExecutionContext) error {
	if err := o.source.Run(execCtx, func(ctx ProduceContext, record RecordBatch) error {
		return o.format.WriteRecord(record)
	}, func(ctx ProduceContext, msg MetadataMessage) error {
		return o.format.WriteMeta(msg)
	}); err != nil {
		return err
	}

	if err := o.format.Close(); err != nil {
		return errors.Wrap(err, "couldn't close output formatter")
	}
	return nil
}

type NativeFormat struct {
	schema physical.Schema
}

func NewNativeFormat(schema physical.Schema) *NativeFormat {
	return &NativeFormat{
		schema: schema,
	}
}

func (n *NativeFormat) WriteRecord(record RecordBatch) error {
	strs := record.String()
	for _, str := range strs {
		fmt.Fprintf(os.Stdout, str+"\n")
	}
	return nil
}

func (n *NativeFormat) WriteMeta(message MetadataMessage) error {
	fmt.Fprintf(os.Stdout, "{~%s}\n", message.Watermark)
	return nil
}

func (n *NativeFormat) SetSchema(schema physical.Schema) {
	n.schema = schema
}

func (n *NativeFormat) Close() error {
	return nil
}

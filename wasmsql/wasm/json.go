package wasm

import (
	"fmt"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"

	"github.com/cube2222/octosql/physical"
)

type JSON struct {
	Schema physical.Schema
}

func (d *JSON) Run(ctx *GenerationContext, produce func(ProduceContext, map[string]VariableMetadata) error) error {
	recordPtr := ctx.AddLocal("record_ptr", wasm.ValueTypeI32)
	filePtr := ctx.AddLocal("file_ptr", wasm.ValueTypeI32)

	localIndices := make([]uint32, len(d.Schema.Fields))
	var recordSize uint32
	for i := range d.Schema.Fields {
		localIndices[i] = ctx.AddLocal(fmt.Sprintf("in_memory_expr_%d", i), wasm.ValueTypeI32) // TODO: Fix type
		// TODO: recordSize += d.Schema.Fields[i].Type.Size()
		recordSize += 4
	}

	// Allocate memory.
	{
		ctx.AppendCode(wasm.OpcodeI32Const)
		ctx.AppendCode(leb128.EncodeUint32(recordSize)...)

		ctx.AppendLibraryCall("sql_malloc")

		ctx.AppendCode(wasm.OpcodeLocalSet)
		ctx.AppendCode(leb128.EncodeUint32(recordPtr)...)
	}
	// TODO: Free memory.

	// Start reading records.
	{
		ctx.AppendCode(wasm.OpcodeI32Const)
		ctx.AppendCode(leb128.EncodeUint32(0)...)

		// call
		ctx.AppendLibraryCall("open_file")

		ctx.AppendCode(wasm.OpcodeLocalSet)
		ctx.AppendCode(leb128.EncodeUint32(filePtr)...)
	}

	// start outer block
	ctx.AppendCode(wasm.OpcodeBlock)
	ctx.AppendCode(leb128.EncodeUint32(ctx.ZeroTypeIndex)...)

	{
		// start loop
		ctx.AppendCode(wasm.OpcodeLoop)
		ctx.AppendCode(leb128.EncodeUint32(ctx.ZeroTypeIndex)...)

		{
			// load new record
			{
				ctx.AppendCode(wasm.OpcodeLocalGet)
				ctx.AppendCode(leb128.EncodeUint32(filePtr)...)

				ctx.AppendCode(wasm.OpcodeLocalGet)
				ctx.AppendCode(leb128.EncodeUint32(recordPtr)...)

				// call
				ctx.AppendLibraryCall("read_json_record")

				// if returned 1 (no more records), break
				ctx.AppendCode(wasm.OpcodeBrIf)
				ctx.AppendCode(leb128.EncodeUint32(1)...) // to block (not loop), which will jump out of it

				// read into local variables
				var valueOffset uint32
				for i := range d.Schema.Fields {
					ctx.AppendCode(wasm.OpcodeI32Const)
					ctx.AppendCode(leb128.EncodeUint32(valueOffset)...)

					ctx.AppendCode(wasm.OpcodeLocalGet)
					ctx.AppendCode(leb128.EncodeUint32(recordPtr)...)

					ctx.AppendCode(wasm.OpcodeI32Add)

					ctx.AppendCode(wasm.OpcodeI32Load, 2, 0)

					ctx.AppendCode(wasm.OpcodeLocalSet)
					ctx.AppendCode(leb128.EncodeUint32(localIndices[i])...)

					valueOffset += 4 // TODO: Fix type size
				}
			}

			varMetadata := make(map[string]VariableMetadata)
			for i := range d.Schema.Fields {
				varMetadata[d.Schema.Fields[i].Name] = VariableMetadata{
					Index: localIndices[i],
				}
			}

			if err := produce(ProduceContext{
				GenerationContext: ctx,
			}, varMetadata); err != nil {
				return err
			}

			// jump to loop start
			ctx.AppendCode(wasm.OpcodeBr)
			ctx.AppendCode(leb128.EncodeUint32(0)...)
		}

		// end loop
		ctx.AppendCode(wasm.OpcodeEnd)
	}

	// end block
	ctx.AppendCode(wasm.OpcodeEnd)

	return nil
}

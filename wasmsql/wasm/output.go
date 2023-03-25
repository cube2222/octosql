package wasm

import (
	"context"
	"fmt"
	"io"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
	"github.com/tetratelabs/wazero/api"
)

type Output struct {
	VariableName string // TODO: Use Expression
	Writer       io.Writer

	Source Node
}

func (o *Output) Run(ctx *GenerationContext) error {
	outputStartPtrIndex := ctx.AddLocal("output_start_ptr", wasm.ValueTypeI32)
	outputPtrIndex := ctx.AddLocal("output_ptr", wasm.ValueTypeI32)

	flushLibraryFunc, flushIndex := ctx.RegisterEnvFunction("flush", func(ctx context.Context, mod api.Module, stack []uint64) {
		memoryStart := uint32(stack[0])
		memoryEnd := uint32(stack[1])

		data, ok := mod.Memory().Read(memoryStart, memoryEnd-memoryStart)
		if !ok {
			panic("problem reading output buffer")
		}
		if _, err := o.Writer.Write(data); err != nil { // Ignoring short writes for now.
			panic(err)
		}
	}, []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, nil)

	// Allocate memory.
	{
		ctx.AppendCode(wasm.OpcodeI32Const)
		ctx.AppendCode(leb128.EncodeUint32(wasm.MemoryPageSize)...)

		ctx.AppendLibraryCall("sql_malloc")

		ctx.AppendCode(wasm.OpcodeLocalSet)
		ctx.AppendCode(leb128.EncodeUint32(outputStartPtrIndex)...)

		ctx.AppendCode(wasm.OpcodeLocalGet)
		ctx.AppendCode(leb128.EncodeUint32(outputStartPtrIndex)...)

		ctx.AppendCode(wasm.OpcodeLocalSet)
		ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)
	}

	if err := o.Source.Run(ctx, func(ctx ProduceContext, variables map[string]VariableMetadata) error {
		varMeta, ok := variables[o.VariableName]
		if !ok {
			return fmt.Errorf("unknown variable: %s", o.VariableName)
		}

		// Flush data if buffer is full
		{
			ctx.AppendCode(wasm.OpcodeBlock)
			ctx.AppendCode(leb128.EncodeUint32(ctx.ZeroTypeIndex)...)

			// push output pointer
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)

			// push buffer end
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputStartPtrIndex)...)
			ctx.AppendCode(wasm.OpcodeI32Const)
			ctx.AppendCode(leb128.EncodeUint32(wasm.MemoryPageSize)...)
			ctx.AppendCode(wasm.OpcodeI32Add)

			// compare
			ctx.AppendCode(wasm.OpcodeI32LtU)

			// if output ptr is smaller than end of buffer, jump out of this block
			ctx.AppendCode(wasm.OpcodeBrIf, 0)

			// push function index
			ctx.AppendCode(wasm.OpcodeI32Const)
			ctx.AppendCode(leb128.EncodeUint32(flushIndex)...)

			// push buffer start
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputStartPtrIndex)...)

			// push output pointer
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)

			// flush buffer
			ctx.AppendLibraryCall(flushLibraryFunc)

			// reset output pointer
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputStartPtrIndex)...)
			ctx.AppendCode(wasm.OpcodeLocalSet)
			ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)

			ctx.AppendCode(wasm.OpcodeEnd)
		}

		// store output
		{
			// get output ptr
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)

			// get value
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(varMeta.Index)...)

			// store value to output
			ctx.AppendCode(wasm.OpcodeI32Store, 2, 0)
		}

		// update output pointer
		{
			// get output pointer
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)

			// add 4
			ctx.AppendCode(wasm.OpcodeI32Const, 4)
			ctx.AppendCode(wasm.OpcodeI32Add)

			// set output pointer
			ctx.AppendCode(wasm.OpcodeLocalSet)
			ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)
		}

		return nil
	}); err != nil {
		return err
	}

	// flush remaining memory
	{
		// push function index
		ctx.AppendCode(wasm.OpcodeI32Const)
		ctx.AppendCode(leb128.EncodeUint32(flushIndex)...)

		// push buffer start
		ctx.AppendCode(wasm.OpcodeLocalGet)
		ctx.AppendCode(leb128.EncodeUint32(outputStartPtrIndex)...)

		// push output pointer
		ctx.AppendCode(wasm.OpcodeLocalGet)
		ctx.AppendCode(leb128.EncodeUint32(outputPtrIndex)...)

		// flush buffer
		ctx.AppendLibraryCall(flushLibraryFunc)
	}

	return nil
}

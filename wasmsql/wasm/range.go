package wasm

import (
	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"

	"github.com/cube2222/octosql/octosql"
)

type Range struct {
	Start, End int32 // TODO: Change to expressions.
}

func (r *Range) Run(ctx *GenerationContext, produce func(ProduceContext, map[string]VariableMetadata) error) error {
	iIndex := ctx.AddLocal("i", wasm.ValueTypeI32)

	// initialize i
	{
		// push range start
		ctx.AppendCode(wasm.OpcodeI32Const)
		ctx.AppendCode(leb128.EncodeInt32(r.Start)...)

		// set i
		ctx.AppendCode(wasm.OpcodeLocalSet)
		ctx.AppendCode(leb128.EncodeUint32(iIndex)...)
	}

	// start loop
	ctx.AppendCode(wasm.OpcodeLoop)
	ctx.AppendCode(leb128.EncodeUint32(ctx.ZeroTypeIndex)...)

	if err := produce(ProduceContext{
		GenerationContext: ctx,
	}, map[string]VariableMetadata{
		"i": {
			ValueMetadata: ValueMetadata{
				Type:     Type(octosql.Int),
				Nullable: false,
			},
			Index: iIndex,
		},
	}); err != nil {
		return err
	}

	// increment i
	{
		// get i
		ctx.AppendCode(wasm.OpcodeLocalGet)
		ctx.AppendCode(leb128.EncodeUint32(iIndex)...)

		// increment
		ctx.AppendCode(
			wasm.OpcodeI32Const, 1,
			wasm.OpcodeI32Add,
		)

		// set i
		ctx.AppendCode(wasm.OpcodeLocalSet)
		ctx.AppendCode(leb128.EncodeUint32(iIndex)...)
	}

	// check if loop should continue
	{
		// get i
		ctx.AppendCode(wasm.OpcodeLocalGet)
		ctx.AppendCode(leb128.EncodeUint32(iIndex)...)

		// push range end
		ctx.AppendCode(wasm.OpcodeI32Const)
		ctx.AppendCode(leb128.EncodeInt32(r.End)...)

		// compare
		ctx.AppendCode(wasm.OpcodeI32LtS)

		// jump
		ctx.AppendCode(wasm.OpcodeBrIf, 0)
	}

	// end loop
	ctx.AppendCode(wasm.OpcodeEnd)

	return nil
}

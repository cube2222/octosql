package wasm

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
	"github.com/tetratelabs/wazero/api"

	"github.com/cube2222/octosql/octosql"
)

type Print struct {
	VariableName string // TODO: Use Expression
	Type         Type

	Source Node
}

func (o *Print) Run(ctx *GenerationContext) error {
	printLibraryFunc, printIndex := ctx.RegisterEnvFunction("print", func(ctx context.Context, mod api.Module, stack []uint64) {
		switch o.Type.TypeID {
		case octosql.TypeIDInt:
			fmt.Println(uint32(stack[0]))
		default:
			panic("unsupported type")
		}
	}, []api.ValueType{api.ValueTypeI32}, nil)

	if err := o.Source.Run(ctx, func(ctx ProduceContext, variables map[string]VariableMetadata) error {
		varMeta, ok := variables[o.VariableName]
		if !ok {
			return fmt.Errorf("unknown variable: %s", o.VariableName)
		}

		// print
		{
			// TODO: The function index should probably go last, for simplicities sake.
			// push function index
			ctx.AppendCode(wasm.OpcodeI32Const)
			ctx.AppendCode(leb128.EncodeUint32(printIndex)...)

			// get value
			ctx.AppendCode(wasm.OpcodeLocalGet)
			ctx.AppendCode(leb128.EncodeUint32(varMeta.Index)...)

			// call
			ctx.AppendLibraryCall(printLibraryFunc)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

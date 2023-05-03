package wasm

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wazero/api"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Print struct {
	Schema physical.Schema
	Source Node
}

// Note:
// So basically in the schema we have the list of fields + types.
// In the variable mapping we have a mapping from schema names to locals.

func (o *Print) Run(ctx *GenerationContext) error {
	printLibraryFunc, printIndex := ctx.RegisterEnvFunction("print", func(ctx context.Context, mod api.Module, stack []uint64) {
		out := ""

		index := uint32(stack[0])
		if index > 0 {
			out += ", "
		}

		switch o.Schema.Fields[index].Type.TypeID {
		case octosql.TypeIDInt:
			out += fmt.Sprintf("%d", uint32(stack[1]))
		default:
			panic("unsupported type")
		}

		if index == uint32(len(o.Schema.Fields)-1) {
			out += "\n"
		}
		fmt.Print(out)
	}, []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, nil)

	if err := o.Source.Run(ctx, func(ctx ProduceContext, variables map[string]VariableMetadata) error {
		for i, field := range o.Schema.Fields {
			varMeta, ok := variables[field.Name]
			if !ok {
				return fmt.Errorf("unknown variable: %s", field.Name)
			}

			printIndex = printIndex
			i = i
			varMeta = varMeta
			printLibraryFunc = printLibraryFunc

			// // TODO: The function index should probably go last, for simplicities sake.
			// // push function index
			// ctx.AppendCode(wasm.OpcodeI32Const)
			// ctx.AppendCode(leb128.EncodeUint32(printIndex)...)
			//
			// // get value
			// ctx.AppendCode(wasm.OpcodeI32Const)
			// ctx.AppendCode(leb128.EncodeUint32(uint32(i))...)
			//
			// // get value
			// ctx.AppendCode(wasm.OpcodeLocalGet)
			// ctx.AppendCode(leb128.EncodeUint32(varMeta.Index)...)
			//
			// // call
			// ctx.AppendLibraryCall(printLibraryFunc)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

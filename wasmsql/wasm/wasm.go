package wasm

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
	"github.com/tetratelabs/wazero/api"

	"github.com/cube2222/octosql/octosql"
)

type Node interface {
	Run(ctx *GenerationContext, produce func(ProduceContext, map[string]VariableMetadata) error) error
}

type Sink interface {
	Run(ctx *GenerationContext) error
}

type ProduceContext struct {
	*GenerationContext
}

// TODO: Change to struct, so that i.e. objects are supported.
type Type octosql.Type

func (t Type) PrimitiveWASMType() wasm.ValueType {
	switch t.TypeID {
	case octosql.TypeIDInt:
		return wasm.ValueTypeI32
	}
	panic("invalid value type")
}

func (t Type) ByteSize() uint32 {
	switch t.TypeID {
	case octosql.TypeIDInt:
		return 4
	}
	panic("invalid value type")
}

type ValueMetadata struct {
	Type     Type
	Nullable bool
}

type VariableMetadata struct {
	Index uint32
}

type FuncArity struct {
	Params  uint32
	Results uint32
}

type GenerationContext struct {
	Code []byte

	Locals     []wasm.ValueType
	LocalNames []string

	FunctionsByArity map[FuncArity][]struct {
		Name string
		Body func(ctx context.Context, mod api.Module, stack []uint64)
	}

	LibraryFunctionIndices map[string]uint32
	ZeroTypeIndex          uint32
}

func (ctx *GenerationContext) RegisterEnvFunction(name string, body func(ctx context.Context, mod api.Module, stack []uint64), params, results []api.ValueType) (string, uint32) {
	arity := FuncArity{
		Params:  uint32(len(params)),
		Results: uint32(len(results)),
	}
	ctx.FunctionsByArity[arity] = append(ctx.FunctionsByArity[arity], struct {
		Name string
		Body func(ctx context.Context, mod api.Module, stack []uint64)
	}{Name: name, Body: body})

	return fmt.Sprintf("hostfunc%d_%d", len(params), len(results)), uint32(len(ctx.FunctionsByArity[arity]) - 1)
}

func (ctx *GenerationContext) AddLocal(name string, valueType wasm.ValueType) uint32 {
	ctx.Locals = append(ctx.Locals, valueType)
	ctx.LocalNames = append(ctx.LocalNames, name)

	// Name is currently unused, but could be added to some kind of debug metadata in the future.
	return uint32(len(ctx.Locals) - 1)
}

func (ctx *GenerationContext) AppendCode(data ...byte) {
	ctx.Code = append(ctx.Code, data...)
}

func (ctx *GenerationContext) AppendLibraryCall(funcName string) {
	ctx.AppendCode(wasm.OpcodeCall)
	funcIndex, ok := ctx.LibraryFunctionIndices[funcName]
	if !ok {
		panic(fmt.Sprintf("unknown library function: %s", funcName))
	}
	ctx.AppendCode(leb128.EncodeUint32(funcIndex)...)
}

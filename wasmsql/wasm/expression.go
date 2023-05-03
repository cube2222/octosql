package wasm

import (
	"fmt"
	"math"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
)

// TODO: Introduce some kind of destructor, so that we i.e. free strings.

type Expression interface {
	Evaluate(*GenerationContext, map[string]VariableMetadata) error
}

type ConstantInteger struct {
	Value int32
}

func (c *ConstantInteger) Evaluate(ctx *GenerationContext, variables map[string]VariableMetadata) error {
	ctx.AppendCode(wasm.OpcodeI32Const)
	ctx.AppendCode(leb128.EncodeInt32(c.Value)...)

	return nil
}

type ConstantFloat struct {
	Value float32
}

func (c *ConstantFloat) Evaluate(ctx *GenerationContext, variables map[string]VariableMetadata) error {
	ctx.AppendCode(wasm.OpcodeI32Const)
	ctx.AppendCode(leb128.EncodeUint32(math.Float32bits(c.Value))...)

	return nil
}

type ReadLocal struct {
	Name string
	// TODO: Type?
}

func (v *ReadLocal) Evaluate(ctx *GenerationContext, variables map[string]VariableMetadata) error {
	varMeta, ok := variables[v.Name]
	if !ok {
		return fmt.Errorf("unknown variable: %s", v.Name)
	}

	ctx.AppendCode(wasm.OpcodeLocalGet)
	ctx.AppendCode(leb128.EncodeUint32(varMeta.Index)...)

	return nil
}

type AddIntegers struct {
	Left, Right Expression
}

func (v *AddIntegers) Evaluate(ctx *GenerationContext, variables map[string]VariableMetadata) error {
	if err := v.Left.Evaluate(ctx, variables); err != nil {
		return fmt.Errorf("couldn't evaluate left argument: %w", err)
	}
	if err := v.Right.Evaluate(ctx, variables); err != nil {
		return fmt.Errorf("couldn't evaluate right argument: %w", err)
	}

	ctx.AppendCode(wasm.OpcodeI32Add)

	return nil
}

type AddFloats struct {
	Left, Right Expression
}

func (v *AddFloats) Evaluate(ctx *GenerationContext, variables map[string]VariableMetadata) error {
	if err := v.Left.Evaluate(ctx, variables); err != nil {
		return fmt.Errorf("couldn't evaluate left argument: %w", err)
	}
	ctx.AppendCode(wasm.OpcodeF32ReinterpretI32)
	if err := v.Right.Evaluate(ctx, variables); err != nil {
		return fmt.Errorf("couldn't evaluate right argument: %w", err)
	}
	ctx.AppendCode(wasm.OpcodeF32ReinterpretI32)

	ctx.AppendCode(wasm.OpcodeF32Add)
	ctx.AppendCode(wasm.OpcodeI32ReinterpretF32)

	return nil
}

type CallBuiltinFunc struct {
	Name      string
	Arguments []Expression
}

func (v *CallBuiltinFunc) Evaluate(ctx *GenerationContext, variables map[string]VariableMetadata) error {
	for _, arg := range v.Arguments {
		if err := arg.Evaluate(ctx, variables); err != nil {
			return fmt.Errorf("couldn't evaluate argument: %w", err)
		}
	}
	ctx.AppendLibraryCall(v.Name)
	return nil
}

// type BooleanExpression interface {
// 	Evaluate(*GenerationContext, map[string]VariableMetadata) error
// }
//
// type BooleanExpressionJumpTargets struct {
// 	IfTrue
// }

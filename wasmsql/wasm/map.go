package wasm

import (
	"fmt"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
	"golang.org/x/exp/maps"

	"github.com/cube2222/octosql/physical"
)

type Map struct {
	Exprs  []Expression
	Schema physical.Schema

	Source Node
}

func (m *Map) Run(ctx *GenerationContext, produce func(ProduceContext, map[string]VariableMetadata) error) error {
	localIndices := make([]uint32, len(m.Exprs))
	for i := range m.Exprs {
		localIndices[i] = ctx.AddLocal(fmt.Sprintf("map_expr_%d", i), Type(m.Schema.Fields[i].Type).PrimitiveWASMType())
	}
	if err := m.Source.Run(ctx, func(ctx ProduceContext, variables map[string]VariableMetadata) error {
		newVariables := maps.Clone(variables)
		for i := range m.Exprs {
			if err := m.Exprs[i].Evaluate(ctx.GenerationContext, variables); err != nil {
				return err
			}
			ctx.AppendCode(wasm.OpcodeLocalSet)
			ctx.AppendCode(leb128.EncodeUint32(localIndices[i])...)

			newVariables[m.Schema.Fields[i].Name] = VariableMetadata{
				Index: localIndices[i],
			}
		}
		return produce(ctx, newVariables)
	}); err != nil {
		return err
	}

	return nil
}

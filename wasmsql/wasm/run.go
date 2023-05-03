package wasm

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	wasmbinary "github.com/tetratelabs/wabin/binary"
	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/wasmsql/rustlib"
)

// TODO: Run wasm-opt on the wasm?

func Run(physicalPlan physical.Node) error {
	ctx := context.Background()

	decodedModule, err := wasmbinary.DecodeModule(rustlib.Module, wasm.CoreFeaturesV2)
	if err != nil {
		return fmt.Errorf("couldn't decode module: %w", err)
	}
	libraryFunctionIndices := map[string]uint32{}
	for _, f := range decodedModule.ExportSection {
		if f.Type != wasm.ExternTypeFunc {
			continue
		}
		libraryFunctionIndices[f.Name] = f.Index
	}
	importedFunctionsSeen := uint32(0)
	for _, f := range decodedModule.ImportSection {
		if f.Type != wasm.ExternTypeFunc {
			continue
		}
		libraryFunctionIndices[f.Name] = importedFunctionsSeen
		importedFunctionsSeen++
	}
	var callCtorsIndex uint32
	var callDtorsIndex uint32
	for _, nameAssociation := range decodedModule.NameSection.FunctionNames {
		if nameAssociation.Name == "__wasm_call_ctors" {
			callCtorsIndex = nameAssociation.Index
		} else if nameAssociation.Name == "__wasm_call_dtors" {
			callDtorsIndex = nameAssociation.Index
		}
	}

	node, err := MaterializeNode(ctx, physicalPlan, physical.Environment{
		VariableContext: &physical.VariableContext{},
	})
	if err != nil {
		return fmt.Errorf("couldn't materialize plan: %w", err)
	}

	// var node Node
	// node = &Range{Start: 1, End: 500000}
	// node = &Map{
	// 	Exprs: []Expression{
	// 		&AddIntegers{
	// 			Left:  &ReadLocal{Name: "i"},
	// 			Right: &ReadLocal{Name: "i"},
	// 		},
	// 	},
	// 	OutFieldName: []string{
	// 		"doubled_i",
	// 	},
	// 	Source: node,
	// }
	// var outputBuf bytes.Buffer
	// sink := &Output{VariableName: "i", Writer: &outputBuf, Source: node}
	sink := &Print{Schema: physicalPlan.Schema, Source: node}
	genCtx := &GenerationContext{
		LibraryFunctionIndices: libraryFunctionIndices,
		ZeroTypeIndex:          uint32(len(decodedModule.TypeSection)),
		FunctionsByArity: map[FuncArity][]struct {
			Name string
			Body func(ctx context.Context, mod api.Module, stack []uint64)
		}{},
	}
	genCtx.AppendCode(wasm.OpcodeCall)
	genCtx.AppendCode(leb128.EncodeUint32(callCtorsIndex)...)
	if err := sink.Run(genCtx); err != nil {
		return err
	}
	genCtx.AppendCode(wasm.OpcodeCall)
	genCtx.AppendCode(leb128.EncodeUint32(callDtorsIndex)...)
	genCtx.AppendCode(wasm.OpcodeEnd)

	// add execution entrypoint
	decodedModule.TypeSection = append(decodedModule.TypeSection, &wasm.FunctionType{})
	decodedModule.FunctionSection = append(decodedModule.FunctionSection, wasm.Index(len(decodedModule.TypeSection)-1))
	decodedModule.CodeSection = append(decodedModule.CodeSection, &wasm.Code{
		LocalTypes: genCtx.Locals,
		Body:       genCtx.Code,
	})
	decodedModule.ExportSection = append(decodedModule.ExportSection, &wasm.Export{
		Name:  "execute",
		Type:  wasm.ExternTypeFunc,
		Index: decodedModule.ImportFuncCount() + uint32(len(decodedModule.FunctionSection)-1),
	})

	var localNames wasm.NameMap
	for i, name := range genCtx.LocalNames {
		localNames = append(localNames, &wasm.NameAssoc{
			Index: wasm.Index(i),
			Name:  name,
		})
	}
	decodedModule.NameSection.LocalNames = append(decodedModule.NameSection.LocalNames, &wasm.NameMapAssoc{
		Index:   wasm.Index(decodedModule.ImportFuncCount() + uint32(len(decodedModule.FunctionSection)) - 1),
		NameMap: localNames,
	})

	bin := wasmbinary.EncodeModule(decodedModule)

	err = os.WriteFile("test.wasm", bin, os.ModePerm)
	if err != nil {
		return err
	}

	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfigCompiler())
	_, err = wasi_snapshot_preview1.Instantiate(ctx, r)
	if err != nil {
		return fmt.Errorf("couldn't instantiate wasi: %w", err)
	}

	envBuilder := r.NewHostModuleBuilder("env")
	for paramCount := uint32(0); paramCount < 3; paramCount++ {
		for resultCount := uint32(0); resultCount < 2; resultCount++ {
			paramCount := paramCount
			resultCount := resultCount
			params := make([]wasm.ValueType, paramCount+1)
			for i := range params {
				params[i] = wasm.ValueTypeI32
			}
			results := make([]wasm.ValueType, resultCount)
			for i := range results {
				results[i] = wasm.ValueTypeI32
			}

			funcs := genCtx.FunctionsByArity[FuncArity{Params: paramCount, Results: resultCount}]

			debug := false
			envBuilder = envBuilder.NewFunctionBuilder().
				WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, mod api.Module, stack []uint64) {
					if debug {
						fmt.Printf("env function '%s(%d) -> %d' called\n", funcs[uint32(stack[0])].Name, paramCount, resultCount)
					}
					var funcStack []uint64
					if paramCount > 0 {
						funcStack = stack[1:]
					}
					funcs[uint32(stack[0])].Body(ctx, mod, funcStack)
					for i := uint32(0); i < resultCount; i++ {
						stack[i] = stack[i+1]
					}
				}), params, results).
				Export(fmt.Sprintf("hostfunc%d_%d", paramCount, resultCount))
		}
	}
	envBuilder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, mod api.Module, stack []uint64) {
			if uint32(stack[0]) == 42 {
				strHeaderData, ok := mod.Memory().Read(uint32(stack[1]), 8)
				if !ok {
					panic("problem reading output buffer")
				}
				strBytesPtr := binary.LittleEndian.Uint32(strHeaderData[:4])
				strBytesLen := binary.LittleEndian.Uint32(strHeaderData[4:])

				strBytes, ok := mod.Memory().Read(strBytesPtr, strBytesLen)
				if !ok {
					panic("problem reading output buffer")
				}
				log.Println("debug 42", string(strBytes))
			} else {
				log.Println("debug", uint32(stack[0]), uint32(stack[1]))
			}

		}), []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI32}).
		Export("debug")
	if _, err := envBuilder.Instantiate(ctx); err != nil {
		return err
	}

	start := time.Now()
	mod, err := r.InstantiateWithConfig(ctx, bin, wazero.NewModuleConfig().WithFSConfig(wazero.NewFSConfig().WithDirMount("/", "/")))
	log.Println("instantiated in", time.Since(start))
	if err != nil {
		return err
	}

	executeFn := mod.ExportedFunction("execute")

	start = time.Now()
	_, err = executeFn.Call(ctx)
	duration := time.Since(start)
	log.Println("elapsed:", duration)
	if err != nil {
		return err
	}
	// data, ok := mod.Memory().Read(0, 64)
	// if !ok {
	// 	return fmt.Errorf("memory read out of range")
	// }
	// spew.Dump(data)

	// outputBytes := outputBuf.Bytes()
	// spew.Dump(len(outputBytes))
	// spew.Dump(outputBytes[:16])

	return nil
}

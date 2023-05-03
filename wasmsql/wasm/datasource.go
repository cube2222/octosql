package wasm

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
	"github.com/tetratelabs/wazero/api"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

// TODO:
//   Should start the datasource on `start_reading_records` - possibly async, maybe in batches.
//   Should somehow pass those to be readable by this datasource.
//   Unfortunately, we have to convert push-based into pull-based here, but this is a low-effort wrapper solution.
//   Natively written datasources could be much faster.
//   Maybe we could try having at least a natively-written JSON datasource?

type Datasource struct {
	Schema physical.Schema
	Source execution.Node
}

func (d *Datasource) Run(ctx *GenerationContext, produce func(ProduceContext, map[string]VariableMetadata) error) error {
	recordPtr := ctx.AddLocal("record_ptr", wasm.ValueTypeI32)

	localIndices := make([]uint32, len(d.Schema.Fields))
	var recordSize uint32
	for i := range d.Schema.Fields {
		localIndices[i] = ctx.AddLocal(fmt.Sprintf("in_memory_expr_%d", i), wasm.ValueTypeI32) // TODO: Fix type
		// TODO: recordSize += d.Schema.Fields[i].Type.Size()
		recordSize += 4
	}

	var cancelDatasource context.CancelFunc = func() {}
	var recordBatchChannel <-chan []execution.Record
	var curBatch []execution.Record
	curBatchRecordIndex := 0

	startReadingLibraryFunc, startReadingIndex := ctx.RegisterEnvFunction("start_reading_records", func(ctx context.Context, mod api.Module, stack []uint64) {
		// TODO: Start a goroutine with the datasource and back-pressure here?
		//   Could send the records in batches of i.e. 1k.
		//   We could then even pass the batches into wasm one batch at a time, without calling the "next_record" function so often.
		//   We have no way to pass values of pushed-down variables (VariableContext), though...
		//   Would need to pass those explicitly somehow.
		cancelDatasource()
		curBatch = nil
		curBatchRecordIndex = 0
		recordBatchChannel, cancelDatasource = d.startDatasource()
	}, nil, nil)

	nextRecordLibraryFunc, nextRecordIndex := ctx.RegisterEnvFunction("next_record", func(ctx context.Context, mod api.Module, stack []uint64) {
		if curBatch == nil || curBatchRecordIndex >= len(curBatch) {
			curBatch = <-recordBatchChannel
			if curBatch == nil {
				stack[0] = 1
				return
			}
			curBatchRecordIndex = 0
		}

		data, ok := mod.Memory().Read(uint32(stack[0]), recordSize)
		if !ok {
			panic("problem reading output buffer")
		}
		var valueOffset uint32
		for i := range d.Schema.Fields {
			value := curBatch[curBatchRecordIndex].Values[i]
			switch d.Schema.Fields[i].Type.TypeID {
			case octosql.TypeIDInt:
				binary.LittleEndian.PutUint32(data[valueOffset:valueOffset+4], uint32(value.Int))
			default:
				panic(fmt.Sprintf("unsupported typeID: %s", d.Schema.Fields[i].Type.TypeID))
			}
			valueOffset += 4 // TODO: Fix type size
		}

		curBatchRecordIndex++
		stack[0] = 0

	}, []api.ValueType{api.ValueTypeI32}, []api.ValueType{api.ValueTypeI32})

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
		ctx.AppendCode(leb128.EncodeUint32(startReadingIndex)...)

		// call
		ctx.AppendLibraryCall(startReadingLibraryFunc)
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
				ctx.AppendCode(wasm.OpcodeI32Const)
				ctx.AppendCode(leb128.EncodeUint32(nextRecordIndex)...)

				ctx.AppendCode(wasm.OpcodeLocalGet)
				ctx.AppendCode(leb128.EncodeUint32(recordPtr)...)

				// call
				ctx.AppendLibraryCall(nextRecordLibraryFunc)

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

func (d *Datasource) startDatasource() (<-chan []execution.Record, context.CancelFunc) {
	recordBatches := make(chan []execution.Record, 8)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(recordBatches)
		batch := make([]execution.Record, 0, 1000)
		if err := d.Source.Run(execution.ExecutionContext{Context: ctx}, func(ctx execution.ProduceContext, record execution.Record) error {
			batch = append(batch, record)

			if len(batch) == cap(batch) {
				select {
				case recordBatches <- batch:
				case <-ctx.Done():
					return ctx.Err()
				}
				batch = make([]execution.Record, 0, 1000)
			}

			return nil
		}, func(ctx execution.ProduceContext, msg execution.MetadataMessage) error { return nil }); err != nil {
			panic(err)
		}
		if len(batch) > 0 {
			select {
			case recordBatches <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	return recordBatches, cancel
}

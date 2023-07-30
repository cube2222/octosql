package json

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/valyala/fastjson"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

const batchSize = 8 * 1024

type ValueReaderFunc func(value *fastjson.Value) error

func ReadJSON(allocator memory.Allocator, r io.Reader, schema *arrow.Schema, produce func(record arrow.Record) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1024*1024*8)

	// All async processing in this function and all jobs created by it use this context.
	// This means that returning from this function will properly clean up all async processors.
	ctx := context.Background()
	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	outChan := make(chan jobOutRecord, 16)

	// This is necessary so that the global worker pool doesn't lock up.
	// We only allow the line reader to create parsing jobs when we know that the output channel has space left.
	// The consumer will always take out a token after reading from the output channel,
	// so filling up the token channel is equivalent to filling up the output channel.
	// But since the line reader goes through the worker pool, it can't block on the output channel.
	// That's why we use a token channel.
	outChanAvailableTokens := make(chan struct{}, 16)

	done := make(chan error, 1)

	// batchesRead is first incremented by the line reader.
	// Then, after the line reader is done, it's read by the consumer,
	// so it knows when it's done reading all the lines.
	// Access is synchronized through the `done` channel.
	batchesRead := 0

	// This routine reads lines and sends them in batches to the parser worker pool.
	go func() {
		line := 0
		job := jobIn{
			sequenceNumber: 0,
			ctx:            localCtx,
			schema:         schema,
			lines:          make([][]byte, 0, batchSize),
			outChan:        outChan,
		}
		for sc.Scan() {
			data := make([]byte, len(sc.Bytes()))
			copy(data, sc.Bytes())
			job.lines = append(job.lines, data)

			if len(job.lines) == batchSize {
				select {
				case outChanAvailableTokens <- struct{}{}:
					parserWorkReceiveChannel <- job
					batchesRead++
				case <-localCtx.Done():
					return
				}
				job = jobIn{
					sequenceNumber: batchesRead,
					ctx:            localCtx,
					schema:         schema,
					lines:          make([][]byte, 0, batchSize),
					outChan:        outChan,
				}
			}

			line++
		}
		if len(job.lines) > 0 {
			select {
			case outChanAvailableTokens <- struct{}{}:
				parserWorkReceiveChannel <- job
				batchesRead++
			case <-localCtx.Done():
				return
			}
		}
		done <- sc.Err()
	}()

	var queue []arrow.Record
	var startIndex int
	var fileReaderIsDone bool
produceLoop:
	for {
		select {
		case out := <-outChan:
			<-outChanAvailableTokens

			if err := out.err; err != nil {
				return fmt.Errorf("couldn't parse json: %w", err)
			}
			for len(queue) <= out.sequenceNumber-startIndex {
				queue = append(queue, nil)
			}
			queue[out.sequenceNumber-startIndex] = out.record
			for len(queue) > 0 && queue[0] != nil {
				record := queue[0]
				if err := produce(record); err != nil {
					return fmt.Errorf("couldn't produce: %w", err)
				}
				queue = queue[1:]
				startIndex++
			}
			if fileReaderIsDone && startIndex == batchesRead {
				break produceLoop
			}
		case readerErr := <-done:
			if readerErr != nil {
				return readerErr
			}
			fileReaderIsDone = true
			done = nil // will block this select branch from now on
			if fileReaderIsDone && startIndex == batchesRead {
				break produceLoop
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func recordReader(schema *arrow.Schema, recordBuilder *array.RecordBuilder) (ValueReaderFunc, error) {
	fields := schema.Fields()
	readers := make([]ValueReaderFunc, len(schema.Fields()))
	for i, field := range fields {
		var err error
		readers[i], err = fieldReader(field, recordBuilder.Field(i))
		if err != nil {
			return nil, fmt.Errorf("couldn't create value reader for field %v: %w", field.Name, err)
		}
	}

	return func(value *fastjson.Value) error {
		obj := value.GetObject()
		for i, field := range fields {
			if err := readers[i](obj.Get(field.Name)); err != nil {
				return fmt.Errorf("couldn't read field %v: %w", field.Name, err)
			}
		}
		return nil
	}, nil
}

func fieldReader(field arrow.Field, builder array.Builder) (ValueReaderFunc, error) {
	var reader ValueReaderFunc
	switch field.Type.ID() {
	case arrow.BOOL:
		reader = boolReader(builder)
	case arrow.INT64:
		reader = intReader(builder)
	case arrow.FLOAT64:
		reader = floatReader(builder)
	case arrow.STRING:
		reader = stringReader(builder)
	case arrow.LIST:
		var err error
		reader, err = listReader(field.Type.(*arrow.ListType).ElemField(), builder)
		if err != nil {
			return nil, fmt.Errorf("couldn't construct list reader: %w", err)
		}
		// TODO: Handle structs and unions. Also, large variants, like LARGE_LIST and LARGE_STRING.
	default:
		return nil, fmt.Errorf("unsupported type: %v", field)
	}

	if field.Nullable {
		reader = nullableReader(builder, reader)
	}
	return reader, nil
}

func boolReader(builder array.Builder) ValueReaderFunc {
	boolBuilder := builder.(*array.BooleanBuilder)
	return func(value *fastjson.Value) error {
		v, err := value.Bool()
		if err != nil {
			return fmt.Errorf("couldn't read bool: %w", err)
		}
		boolBuilder.Append(v)
		return nil
	}
}

func intReader(builder array.Builder) ValueReaderFunc {
	intBuilder := builder.(*array.Int64Builder)
	return func(value *fastjson.Value) error {
		v, err := value.Int64()
		if err != nil {
			return fmt.Errorf("couldn't read float: %w", err)
		}
		intBuilder.Append(v)
		return nil
	}
}

func floatReader(builder array.Builder) ValueReaderFunc {
	floatBuilder := builder.(*array.Float64Builder)
	return func(value *fastjson.Value) error {
		v, err := value.Float64()
		if err != nil {
			return fmt.Errorf("couldn't read float: %w", err)
		}
		floatBuilder.Append(v)
		return nil
	}
}

func stringReader(builder array.Builder) ValueReaderFunc {
	stringBuilder := builder.(*array.StringBuilder)
	return func(value *fastjson.Value) error {
		v, err := value.StringBytes()
		if err != nil {
			return fmt.Errorf("couldn't read string: %w", err)
		}
		stringBuilder.BinaryBuilder.Append(v)
		return nil
	}
}

func listReader(field arrow.Field, builder array.Builder) (ValueReaderFunc, error) {
	listBuilder := builder.(*array.ListBuilder)
	valueReader, err := fieldReader(field, listBuilder.ValueBuilder())
	if err != nil {
		panic(err)
	}
	return func(value *fastjson.Value) error {
		listBuilder.Append(true)
		list, err := value.Array()
		if err != nil {
			return fmt.Errorf("couldn't read json array: %w", err)
		}
		for i := 0; i < len(list); i++ {
			if err := valueReader(list[i]); err != nil {
				return fmt.Errorf("couldn't read list element: %w", err)
			}
		}
		return nil
	}, nil
}

func nullableReader(builder array.Builder, reader ValueReaderFunc) ValueReaderFunc {
	return func(value *fastjson.Value) error {
		if value == nil || value.Type() == fastjson.TypeNull {
			builder.AppendNull()
			return nil
		}
		return reader(value)
	}
}

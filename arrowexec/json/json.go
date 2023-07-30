package json

import (
	"bufio"
	"fmt"
	"io"

	"github.com/valyala/fastjson"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

const batchSize = 32 * 1024

type ValueReaderFunc func(value *fastjson.Value) error

func ReadJSON(allocator memory.Allocator, r io.Reader, schema *arrow.Schema, produce func(record arrow.Record) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1024*1024*8)

	recordBuilder := array.NewRecordBuilder(allocator, schema)
	recordBuilder.Reserve(batchSize)

	readerFunc, err := recordReader(schema, recordBuilder)
	if err != nil {
		return fmt.Errorf("couldn't construct record reader function: %w", err)
	}

	var p fastjson.Parser
	count := 0
	for sc.Scan() {
		line := sc.Bytes()
		value, err := p.ParseBytes(line)
		if err != nil {
			return err
		}
		if err := readerFunc(value); err != nil {
			return fmt.Errorf("couldn't read record: %w", err)
		}
		count++
		if count == batchSize {
			record := recordBuilder.NewRecord()
			if err := produce(record); err != nil {
				return fmt.Errorf("couldn't produce record: %w", err)
			}
			record.Release()
			count = 0
			recordBuilder.Reserve(batchSize)
		}
	}

	if count > 0 {
		record := recordBuilder.NewRecord()
		if err := produce(record); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
		record.Release()
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("couldn't read line: %w", err)
	}

	return nil
}

func recordReader(schema *arrow.Schema, recordBuilder *array.RecordBuilder) (ValueReaderFunc, error) {
	fields := schema.Fields()
	readers := make([]ValueReaderFunc, len(schema.Fields()))
	for i, field := range fields {
		var err error
		readers[i], err = valueReader(field, recordBuilder.Field(i))
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

func valueReader(dt arrow.Field, builder array.Builder) (ValueReaderFunc, error) {
	var reader ValueReaderFunc
	switch dt.Type.ID() {
	case arrow.BOOL:
		reader = boolReader(builder)
	case arrow.INT64:
		reader = intReader(builder)
	case arrow.FLOAT64:
		reader = floatReader(builder)
	case arrow.STRING:
		reader = stringReader(builder)
		// TODO: Handle structs, lists, and unions.
	default:
		return nil, fmt.Errorf("unsupported type: %v", dt)
	}

	if dt.Nullable {
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

func nullableReader(builder array.Builder, reader ValueReaderFunc) ValueReaderFunc {
	return func(value *fastjson.Value) error {
		if value == nil || value.Type() == fastjson.TypeNull {
			builder.AppendNull()
			return nil
		}
		return reader(value)
	}
}

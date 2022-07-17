package json

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/valyala/fastjson"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/files"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	path   string
	tail   bool
	fields []physical.SchemaField
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	var reader io.Reader
	if !d.tail {
		f, err := os.Open(d.path)
		if err != nil {
			return fmt.Errorf("couldn't open file: %w", err)
		}
		defer f.Close()
		reader = bufio.NewReaderSize(f, 4096*1024)
	} else {
		r, err := files.Tail(ctx, d.path)
		if err != nil {
			return fmt.Errorf("couldn't tail file: %w", err)
		}
		defer r.Close()
		reader = r
	}

	sc := bufio.NewScanner(reader)
	sc.Buffer(nil, 1024*1024)

	var p fastjson.Parser
	for sc.Scan() {
		v, err := p.ParseBytes(sc.Bytes())
		if err != nil {
			return fmt.Errorf("couldn't parse json: %w", err)
		}
		if v.Type() != fastjson.TypeObject {
			return fmt.Errorf("expected JSON object, got '%s'", sc.Text())
		}
		o, err := v.Object()
		if err != nil {
			return fmt.Errorf("expected JSON object, got '%s'", sc.Text())
		}

		values := make([]octosql.Value, len(d.fields))
		for i := range values {
			values[i], _ = getOctoSQLValue(d.fields[i].Type, o.Get(d.fields[i].Name))
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return sc.Err()
}

func getOctoSQLValue(t octosql.Type, value *fastjson.Value) (out octosql.Value, ok bool) {
	if value == nil {
		return octosql.NewNull(), t.TypeID == octosql.TypeIDNull
	}

	switch t.TypeID {
	case octosql.TypeIDFloat:
		if value.Type() == fastjson.TypeNumber {
			v, _ := value.Float64()
			return octosql.NewFloat(v), true
		}
	case octosql.TypeIDBoolean:
		if value.Type() == fastjson.TypeTrue {
			return octosql.NewBoolean(true), true
		} else if value.Type() == fastjson.TypeFalse {
			return octosql.NewBoolean(false), true
		}
	case octosql.TypeIDString:
		if value.Type() == fastjson.TypeString {
			v, _ := value.StringBytes()
			return octosql.NewString(string(v)), true
		}
	case octosql.TypeIDTime:
		if value.Type() == fastjson.TypeString {
			v, _ := value.StringBytes()
			if parsed, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
				return octosql.NewTime(parsed), true
			}
		}
	case octosql.TypeIDDuration:
		if value.Type() == fastjson.TypeString {
			v, _ := value.StringBytes()
			if parsed, err := time.ParseDuration(string(v)); err == nil {
				return octosql.NewDuration(parsed), true
			}
		}
	case octosql.TypeIDList:
		if value.Type() == fastjson.TypeArray {
			arr, _ := value.Array()
			values := make([]octosql.Value, len(arr))

			outOk := true
			for i := range arr {
				curValue, curOk := getOctoSQLValue(*t.List.Element, arr[i])
				values[i] = curValue
				outOk = outOk && curOk
			}
			return octosql.NewList(values), outOk
		}
	case octosql.TypeIDStruct:
		if value.Type() == fastjson.TypeObject {
			obj, _ := value.Object()
			values := make([]octosql.Value, len(t.Struct.Fields))

			outOk := true
			for i, field := range t.Struct.Fields {
				curValue, curOk := getOctoSQLValue(field.Type, obj.Get(field.Name))
				values[i] = curValue
				outOk = outOk && curOk
			}
			return octosql.NewStruct(values), outOk
		}
	case octosql.TypeIDUnion:
		for _, alternative := range t.Union.Alternatives {
			v, ok := getOctoSQLValue(alternative, value)
			if ok {
				return v, true
			}
		}
	}

	return octosql.ZeroValue, false
}

package json

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/segmentio/encoding/json"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	path   string
	fields []physical.SchemaField
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	f, err := os.Open(d.path)
	if err != nil {
		return fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)

	for {
		var msg map[string]interface{}
		if err := decoder.Decode(&msg); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("couldn't decode message: %w", err)
		}

		values := make([]octosql.Value, len(d.fields))
		for i := range values {
			values[i], _ = getOctoSQLValue(d.fields[i].Type, msg[d.fields[i].Name])
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
}

func getOctoSQLValue(t octosql.Type, value interface{}) (out octosql.Value, ok bool) {
	switch t.TypeID {
	case octosql.TypeIDNull:
		if value == nil {
			return octosql.NewNull(), true
		}
	case octosql.TypeIDInt:
		if value, ok := value.(int); ok {
			return octosql.NewInt(value), true
		}
	case octosql.TypeIDFloat:
		if value, ok := value.(float64); ok {
			return octosql.NewFloat(value), true
		}
	case octosql.TypeIDBoolean:
		if value, ok := value.(bool); ok {
			return octosql.NewBoolean(value), true
		}
	case octosql.TypeIDString:
		if value, ok := value.(string); ok {
			return octosql.NewString(value), true
		}
	case octosql.TypeIDTime:
		if value, ok := value.(string); ok {
			if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
				return octosql.NewTime(parsed), true
			}
		}
	case octosql.TypeIDDuration:
		if value, ok := value.(string); ok {
			if parsed, err := time.ParseDuration(value); err == nil {
				return octosql.NewDuration(parsed), true
			}
		}
	case octosql.TypeIDList:
		if value, ok := value.([]interface{}); ok {
			elements := make([]octosql.Value, len(value))
			outOk := true
			for i := range elements {
				curElement, curOk := getOctoSQLValue(*t.List.Element, value[i])
				elements[i] = curElement
				outOk = outOk && curOk
			}
			return octosql.NewList(elements), outOk
		}
	case octosql.TypeIDStruct:
		if value, ok := value.(map[string]interface{}); ok {
			values := make([]octosql.Value, len(t.Struct.Fields))
			outOk := true
			for i, field := range t.Struct.Fields {
				curValue, curOk := getOctoSQLValue(field.Type, value[field.Name])
				values[i] = curValue
				outOk = outOk && curOk
			}
			return octosql.NewStruct(values), outOk
		}
	case octosql.TypeIDTuple:
		if value, ok := value.([]interface{}); ok {
			elements := make([]octosql.Value, len(value))
			outOk := true
			for i := range elements {
				curElement, curOk := getOctoSQLValue(t.Tuple.Elements[i], value[i])
				elements[i] = curElement
				outOk = outOk && curOk
			}
			return octosql.NewList(elements), outOk
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

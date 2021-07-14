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
	decoder.ZeroCopy()

	for {
		var msg map[string]interface{}
		if err := decoder.Decode(&msg); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("couldn't decode message: %w", err)
		}

		values := make([]octosql.Value, len(d.fields))
		for i := range values {
			values[i] = getOctoSQLValue(d.fields[i].Type, msg[d.fields[i].Name])
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
}

func getOctoSQLValue(t octosql.Type, value interface{}) octosql.Value {
	switch value := value.(type) {
	case int:
		return octosql.NewInt(value)
	case bool:
		return octosql.NewBoolean(value)
	case float64:
		return octosql.NewFloat(value)
	case string:
		// TODO: this should happen based on the schema only.
		if t.Is(octosql.Time) >= octosql.TypeRelationMaybe {
			if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
				return octosql.NewTime(t)
			} else {
				return octosql.NewString(value)
			}
		}
		return octosql.NewString(value)
	case time.Time:
		return octosql.NewTime(value)
	case map[string]interface{}:
		// TODO: This won't work if we have structs with varying fields.
		// We have to look at the type and base the names on that.
		values := make([]octosql.Value, len(t.Struct.Fields))
		for i, field := range t.Struct.Fields {
			values[i] = getOctoSQLValue(field.Type, value[field.Name])
		}
		names := make([]string, len(t.Struct.Fields))
		for i := range t.Struct.Fields {
			names[i] = t.Struct.Fields[i].Name
		}
		return octosql.NewStruct(names, values)
	case []interface{}:
		elements := make([]octosql.Value, len(value))
		for i := range elements {
			// TODO: This will not work for only empty lists.
			// TODO: This will also not work for T1 | List<T2>, same for the structs above actually.
			// TODO: Types just shouldn't be discriminated unions.
			elements[i] = getOctoSQLValue(*t.List.Element, value[i])
		}
		return octosql.NewList(elements)
	case nil:
		return octosql.NewNull()
	}

	panic(fmt.Sprintf("unexhaustive json input value match: %T %+v", value, value))
}

package json

import (
	"fmt"
	"io"
	"os"
	"sort"
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
			values[i] = getOctoSQLValue(msg[d.fields[i].Name])
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false)); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
}

func getOctoSQLValue(value interface{}) octosql.Value {
	switch value := value.(type) {
	case int:
		return octosql.NewInt(value)
	case bool:
		return octosql.NewBoolean(value)
	case float64:
		return octosql.NewFloat(value)
	case string:
		// TODO: this should happen based on the schema only.
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return octosql.NewTime(t)
		} else {
			return octosql.NewString(value)
		}
	case time.Time:
		return octosql.NewTime(value)
	case map[string]interface{}:
		// TODO: This won't work if we have structs with varying fields.
		// We have to look at the type and base the names on that.
		fieldNames := make([]string, 0, len(value))
		for k := range value {
			fieldNames = append(fieldNames, k)
		}
		sort.Strings(fieldNames)
		values := make([]octosql.Value, len(value))
		for i := range fieldNames {
			values[i] = getOctoSQLValue(value[fieldNames[i]])
		}
		return octosql.NewStruct(fieldNames, values)
	case []interface{}:
		elements := make([]octosql.Value, len(value))
		for i := range elements {
			elements[i] = getOctoSQLValue(value[i])
		}
		return octosql.NewList(elements)
	case nil:
		return octosql.NewNull()
	}

	panic(fmt.Sprintf("unexhaustive json input value match: %T %+v", value, value))
}

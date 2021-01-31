package json

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/cube2222/octosql"
	. "github.com/cube2222/octosql/execution"
)

type Datasource struct {
	Path   string
	Fields []string
}

func (d *Datasource) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	f, err := os.Open(d.Path)
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

		values := make([]octosql.Value, len(d.Fields))
		for i := range values {
			value := msg[d.Fields[i]]
			switch value := value.(type) {
			case int:
				values[i] = octosql.NewInt(value)
			case bool:
				values[i] = octosql.NewBoolean(value)
			case float64:
				values[i] = octosql.NewFloat(value)
			case string:
				if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
					values[i] = octosql.NewTime(t)
				} else {
					values[i] = octosql.NewString(value)
				}
			case time.Time:
				values[i] = octosql.NewTime(value)
				// TODO: Parse lists.
				// TODO: Parse nested objects.
			}
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false)); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
}

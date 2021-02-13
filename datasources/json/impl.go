package json

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(name string) (physical.DatasourceImplementation, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	decoder.ZeroCopy()

	fields := make(map[string]octosql.Type)

	for i := 0; i < 10; i++ {
		var msg map[string]interface{}
		if err := decoder.Decode(&msg); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("couldn't decode message: %w", err)
		}

		for k := range msg {
			value := msg[k]
			switch value := value.(type) {
			case int:
				if t, ok := fields[k]; !ok {
					fields[k] = octosql.TypeSum(t, octosql.Int)
				} else {
					fields[k] = octosql.Int
				}
			case bool:
				if t, ok := fields[k]; ok {
					fields[k] = octosql.TypeSum(t, octosql.Boolean)
				} else {
					fields[k] = octosql.Boolean
				}
			case float64:
				if t, ok := fields[k]; ok {
					fields[k] = octosql.TypeSum(t, octosql.Float)
				} else {
					fields[k] = octosql.Float
				}
			case string:
				if _, err := time.Parse(time.RFC3339Nano, value); err == nil {
					if t, ok := fields[k]; ok {
						fields[k] = octosql.TypeSum(t, octosql.Time)
					} else {
						fields[k] = octosql.Time
					}
				} else {
					if t, ok := fields[k]; ok {
						fields[k] = octosql.TypeSum(t, octosql.String)
					} else {
						fields[k] = octosql.String
					}
				}
			case time.Time:
				if t, ok := fields[k]; ok {
					fields[k] = octosql.TypeSum(t, octosql.Time)
				} else {
					fields[k] = octosql.Time
				}
			// TODO: Handle lists.
			// TODO: Handle nested objects.
			case nil:
				if t, ok := fields[k]; ok {
					fields[k] = octosql.TypeSum(t, octosql.Null)
				} else {
					fields[k] = octosql.Null
				}
			}
		}
	}

	var schemaFields []physical.SchemaField
	for k, t := range fields {
		schemaFields = append(schemaFields, physical.SchemaField{
			Name: k,
			Type: t,
		})
	}

	return &impl{
		path:   name,
		schema: physical.NewSchema(schemaFields, -1),
	}, nil
}

type impl struct {
	path   string
	schema physical.Schema
}

func (i *impl) Schema() (physical.Schema, error) {
	return i.schema, nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment) (execution.Node, error) {
	return &DatasourceExecuting{
		Path:   i.path,
		Fields: i.schema.Fields,
	}, nil
}

func (i *impl) PushDownPredicates(ctx context.Context, env physical.Environment, newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

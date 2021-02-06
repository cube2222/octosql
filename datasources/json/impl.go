package json

import (
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
					fields[k] = octosql.Int
				} else if octosql.Int.Is(t) != octosql.TypeRelationIs {
					fields[k] = octosql.Int // TODO: Should be union of old and new. ClosestCommonType function.
				}
			case bool:
				if t, ok := fields[k]; !ok {
					fields[k] = octosql.Boolean
				} else if octosql.Boolean.Is(t) != octosql.TypeRelationIs {
					fields[k] = octosql.Boolean // TODO: Should be union of old and new. ClosestCommonType function.
				}
			case float64:
				if t, ok := fields[k]; !ok {
					fields[k] = octosql.Float
				} else if octosql.Float.Is(t) != octosql.TypeRelationIs {
					fields[k] = octosql.Float // TODO: Should be union of old and new. ClosestCommonType function.
				}
			case string:
				// TODO: this should happen based on the schema only.
				if _, err := time.Parse(time.RFC3339Nano, value); err == nil {
					if t, ok := fields[k]; !ok {
						fields[k] = octosql.Time
					} else if octosql.Time.Is(t) != octosql.TypeRelationIs {
						fields[k] = octosql.Time // TODO: Should be union of old and new. ClosestCommonType function.
					}
				} else {
					if t, ok := fields[k]; !ok {
						fields[k] = octosql.String
					} else if octosql.String.Is(t) != octosql.TypeRelationIs {
						fields[k] = octosql.String // TODO: Should be union of old and new. ClosestCommonType function.
					}
				}
			case time.Time:
				if t, ok := fields[k]; !ok {
					fields[k] = octosql.Time
				} else if octosql.Time.Is(t) != octosql.TypeRelationIs {
					fields[k] = octosql.Time // TODO: Should be union of old and new. ClosestCommonType function.
				}
				// TODO: Handle lists.
				// TODO: Handle nested objects.
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

func (i *impl) Materialize() (execution.Node, error) {
	return &DatasourceExecuting{
		Path:   i.path,
		Fields: i.schema.Fields,
	}, nil
}

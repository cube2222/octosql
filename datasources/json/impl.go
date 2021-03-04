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
			if t, ok := fields[k]; !ok {
				fields[k] = octosql.TypeSum(t, getOctoSQLType(msg[k]))
			} else {
				fields[k] = getOctoSQLType(msg[k])
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

func getOctoSQLType(value interface{}) octosql.Type {
	switch value := value.(type) {
	case int:
		return octosql.Int
	case bool:
		return octosql.Boolean
	case float64:
		return octosql.Float
	case string:
		if _, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return octosql.Time
		} else {
			return octosql.String
		}
	case time.Time:
		return octosql.Time
	// TODO: Handle nested objects.
	case []interface{}:
		var elementType *octosql.Type
		for i := range value {
			if elementType != nil {
				t := octosql.TypeSum(*elementType, getOctoSQLType(value[i]))
				elementType = &t
			} else {
				t := getOctoSQLType(value[i])
				elementType = &t
			}
		}
		// TODO: Ticking time bomb, because we don't currently handle the case where this is null, because the list is empty.
		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{
				Element: elementType,
			},
		}

	case nil:
		return octosql.Null
	}

	panic(fmt.Sprintf("unexhaustive json input value match: %T %+v", value, value))
}

type impl struct {
	path   string
	schema physical.Schema
}

func (i *impl) Schema() (physical.Schema, error) {
	return i.schema, nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:   i.path,
		fields: i.schema.Fields,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

package json

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(name string) (physical.DatasourceImplementation, physical.Schema, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't open file: %w", err)
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
			return nil, physical.Schema{}, fmt.Errorf("couldn't decode message: %w", err)
		}

		for k := range msg {
			if t, ok := fields[k]; ok {
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
	sort.Slice(schemaFields, func(i, j int) bool {
		return schemaFields[i].Name < schemaFields[j].Name
	})

	return &impl{
			path: name,
		},
		physical.NewSchema(schemaFields, -1),
		nil
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
	case map[string]interface{}:
		fieldNames := make([]string, 0, len(value))
		for k := range value {
			fieldNames = append(fieldNames, k)
		}
		sort.Strings(fieldNames)
		fields := make([]octosql.StructField, len(value))
		for i := range fieldNames {
			fields[i] = octosql.StructField{
				Name: fieldNames[i],
				Type: getOctoSQLType(value[fieldNames[i]]),
			}
		}
		return octosql.Type{
			TypeID: octosql.TypeIDStruct,
			Struct: struct{ Fields []octosql.StructField }{Fields: fields},
		}
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
	path string
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:   i.path,
		fields: schema.Fields,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

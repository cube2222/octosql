package json

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/valyala/fastjson"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()

	fields := make(map[string]octosql.Type)

	sc := bufio.NewScanner(bufio.NewReaderSize(f, 4096*1024))

	var p fastjson.Parser
	i := 0
	for sc.Scan() && i < 100 {
		i++
		v, err := p.ParseBytes(sc.Bytes())
		if err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't parse json: %w", err)
		}
		if v.Type() != fastjson.TypeObject {
			return nil, physical.Schema{}, fmt.Errorf("expected JSON object, got '%s'", sc.Text())
		}
		o, err := v.Object()
		if err != nil {
			return nil, physical.Schema{}, fmt.Errorf("expected JSON object, got '%s'", sc.Text())
		}

		o.Visit(func(key []byte, v *fastjson.Value) {
			if t, ok := fields[string(key)]; ok {
				fields[string(key)] = octosql.TypeSum(t, getOctoSQLType(v))
			} else {
				fields[string(key)] = getOctoSQLType(v)
			}
		})
	}
	if sc.Err() != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't scan lines: %w", sc.Err())
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
		physical.NewSchema(schemaFields, -1, physical.WithNoRetractions(true)),
		nil
}

func getOctoSQLType(value *fastjson.Value) octosql.Type {
	switch value.Type() {
	case fastjson.TypeNull:
		return octosql.Null
	case fastjson.TypeString:
		v, _ := value.StringBytes()
		if _, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
			return octosql.Time
		} else {
			return octosql.String
		}
	case fastjson.TypeNumber:
		return octosql.Float
	case fastjson.TypeTrue, fastjson.TypeFalse:
		return octosql.Boolean
	case fastjson.TypeObject:
		obj, _ := value.Object()
		fields := make([]octosql.StructField, 0, obj.Len())
		obj.Visit(func(key []byte, v *fastjson.Value) {
			fields = append(fields, octosql.StructField{
				Name: string(key),
				Type: getOctoSQLType(v),
			})
		})
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Name < fields[j].Name
		})
		return octosql.Type{
			TypeID: octosql.TypeIDStruct,
			Struct: struct{ Fields []octosql.StructField }{Fields: fields},
		}
	case fastjson.TypeArray:
		arr, _ := value.Array()
		var elementType *octosql.Type
		for i := range arr {
			if elementType != nil {
				t := octosql.TypeSum(*elementType, getOctoSQLType(arr[i]))
				elementType = &t
			} else {
				t := getOctoSQLType(arr[i])
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
	}

	panic(fmt.Sprintf("unexhaustive json input value match: %s %+v", value.Type().String(), value))
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

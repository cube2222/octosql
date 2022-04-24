package parquet

import (
	"context"
	"fmt"
	"os"

	"github.com/segmentio/parquet-go"

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
	stat, err := f.Stat()
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't stat file: %w", err)
	}

	pr, err := parquet.OpenFile(f, stat.Size(), &parquet.FileConfig{
		SkipPageIndex:    true,
		SkipBloomFilters: true,
	})
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't open parquet file: %w", err)
	}
	schema := pr.Schema()
	schemaFields := schema.Fields()
	outSchemaFields := make([]physical.SchemaField, len(schemaFields))
	for i, field := range schemaFields {
		fieldName, fieldType := getOctoSQLField(field)
		outSchemaFields[i] = physical.SchemaField{
			Name: fieldName,
			Type: fieldType,
		}
	}

	return &impl{
			path: name,
		},
		physical.NewSchema(outSchemaFields, -1, physical.WithNoRetractions(true)),
		nil
}

func getOctoSQLField(field parquet.Field) (string, octosql.Type) {
	return field.Name(), getOctoSQLNode(field)
}

func getOctoSQLNode(node parquet.Node) octosql.Type {
	var outType octosql.Type
	if node.Leaf() {
		switch node.Type().Kind() {
		case parquet.Boolean:
			outType = octosql.Boolean
		case parquet.Int32:
			outType = octosql.Int
		case parquet.Int64:
			outType = octosql.Int
		case parquet.Float:
			outType = octosql.Float
		case parquet.Double:
			outType = octosql.Float
		case parquet.ByteArray:
			outType = octosql.String
		case parquet.FixedLenByteArray:
			outType = octosql.String
		}
	} else {
		switch {
		case isList(node):
			elem := listElementOf(node)
			elemType := getOctoSQLNode(elem)
			outType = octosql.Type{
				TypeID: octosql.TypeIDList,
				List:   struct{ Element *octosql.Type }{Element: &elemType},
			}
		default:
			var fields []octosql.StructField
			for _, child := range node.Fields() {
				childName, childType := getOctoSQLField(child)
				fields = append(fields, octosql.StructField{
					Name: childName,
					Type: childType,
				})
			}
			outType = octosql.Type{
				TypeID: octosql.TypeIDStruct,
				Struct: struct {
					Fields []octosql.StructField
				}{
					Fields: fields,
				},
			}
		}
	}

	if node.Optional() {
		outType = octosql.TypeSum(outType, octosql.Null)
	} else if node.Repeated() {
		elemType := outType
		outType = octosql.Type{
			TypeID: octosql.TypeIDList,
			List:   struct{ Element *octosql.Type }{Element: &elemType},
		}
	}

	return outType
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

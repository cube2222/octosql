package slicedb

import (
	"context"
	"fmt"
	"reflect"

	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Database struct {
	tables map[string]physical.DatasourceImplementation
}

func NewDatabase(tables map[string]interface{}) (*Database, error) {
	tableDescriptors := map[string]physical.DatasourceImplementation{}
	for name, table := range tables {
		if reflect.TypeOf(table).Kind() != reflect.Slice {
			return nil, fmt.Errorf("table %s is not a slice, is: %T", name, table)
		}

		tableValue := reflect.ValueOf(table)

		sampleSize := tableValue.Len()
		if sampleSize > 10 {
			sampleSize = 10
		}

		tableType := octosql.Type{
			TypeID: octosql.TypeIDStruct,
		}
		for i := 0; i < tableValue.Len(); i++ {
			value := tableValue.Index(i)
			if value.Kind() != reflect.Struct {
				return nil, fmt.Errorf("table %s elements must be structs, got %T", name, value.Type().String())
			}

			typename := getTypeFromReflectType(value.Type())
			tableType = octosql.TypeSum(tableType, typename)
		}
		fields := make([]physical.SchemaField, len(tableType.Struct.Fields))
		for i := range tableType.Struct.Fields {
			fields[i] = physical.SchemaField{
				Name: tableType.Struct.Fields[i].Name,
				Type: tableType.Struct.Fields[i].Type,
			}
		}
		tableDescriptors[name] = &impl{
			schema: physical.Schema{
				Fields:    fields,
				TimeField: -1,
			},
			table: name,
		}
	}

	return &Database{
		tables: tableDescriptors,
	}, nil
}

func getTypeFromReflectType(t reflect.Type) octosql.Type {
	switch t.Kind() {
	case reflect.Bool:
		return octosql.Boolean
	case reflect.Int:
		return octosql.Int
	case reflect.Float64:
		return octosql.Float
	// case reflect.Array:
	case reflect.Ptr:
		return octosql.TypeSum(getTypeFromReflectType(t.Elem()), octosql.Null)
	case reflect.Slice:
		typename := getTypeFromReflectType(t.Elem())
		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{
				Element: &typename,
			},
		}
	case reflect.String:
		return octosql.String
	case reflect.Struct:
		fieldCount := t.NumField()
		fields := make([]octosql.StructField, fieldCount)
		for i := 0; i < fieldCount; i++ {
			field := t.Field(i)

			fields[i] = octosql.StructField{
				Name: field.Name,
				Type: getTypeFromReflectType(field.Type),
			}
		}
		return octosql.Type{
			TypeID: octosql.TypeIDStruct,
			Struct: struct{ Fields []octosql.StructField }{Fields: fields},
		}
	default:
		// TODO: Test if array catches this.
		panic(fmt.Sprintf("unsupported field type: %s", t.String()))
	}
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, error) {
	table, ok := d.tables[name]
	if !ok {
		return nil, fmt.Errorf("unknown table")
	}
	return table, nil
}

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
	tables  map[string]physical.DatasourceImplementation
	schemas map[string]physical.Schema
}

func NewDatabase(tables map[string]interface{}) (*Database, error) {
	tableDescriptors := map[string]physical.DatasourceImplementation{}
	schemas := map[string]physical.Schema{}
	for name, table := range tables {
		if reflect.TypeOf(table).Kind() != reflect.Slice {
			return nil, fmt.Errorf("table %s is not a slice, is: %T", name, table)
		}

		tableValue := reflect.ValueOf(table)

		sampleSize := tableValue.Len()
		if sampleSize > 10 {
			sampleSize = 10
		}

		var tableType *octosql.Type
		for i := 0; i < tableValue.Len(); i++ {
			value := tableValue.Index(i)
			if value.Kind() != reflect.Struct {
				return nil, fmt.Errorf("table %s elements must be structs, got %T", name, value.Type().String())
			}

			typename := getTypeFromReflectValue(value)
			if tableType == nil {
				tableType = &typename
			} else {
				newType := octosql.TypeSum(*tableType, typename)
				tableType = &newType
			}
		}
		fields := make([]physical.SchemaField, len(tableType.Struct.Fields))
		for i := range tableType.Struct.Fields {
			fields[i] = physical.SchemaField{
				Name: tableType.Struct.Fields[i].Name,
				Type: tableType.Struct.Fields[i].Type,
			}
		}
		tableDescriptors[name] = &impl{
			values: table,
		}
		schemas[name] = physical.Schema{
			Fields:    fields,
			TimeField: -1,
		}
	}

	return &Database{
		tables: tableDescriptors,
	}, nil
}

// TODO: Handle Time and Duration.
func getTypeFromReflectValue(v reflect.Value) octosql.Type {
	switch v.Kind() {
	case reflect.Bool:
		return octosql.Boolean
	case reflect.Int:
		return octosql.Int
	case reflect.Float64:
		return octosql.Float
	// case reflect.Array:
	case reflect.Ptr:
		if v.IsNil() {
			return octosql.Null
		}
		return octosql.TypeSum(getTypeFromReflectValue(v.Elem()), octosql.Null)
	case reflect.Slice:
		typename := getTypeFromReflectValue(v.Elem())
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
		fieldCount := v.NumField()
		fields := make([]octosql.StructField, 0, fieldCount)
		for i := 0; i < fieldCount; i++ {
			if !v.Field(i).CanInterface() {
				continue
			}
			name := v.Type().Field(i).Name
			if alias, ok := v.Type().Field(i).Tag.Lookup("slicequery"); ok {
				name = alias
			}
			fields = append(fields, octosql.StructField{
				Name: name,
				Type: getTypeFromReflectValue(v.Field(i)),
			})
		}
		return octosql.Type{
			TypeID: octosql.TypeIDStruct,
			Struct: struct{ Fields []octosql.StructField }{Fields: fields},
		}
	case reflect.Interface:
		return getTypeFromReflectValue(v.Elem())
	default:
		// TODO: Test if array catches this.
		panic(fmt.Sprintf("unsupported field type: %s", v.Type()))
	}
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	table, ok := d.tables[name]
	if !ok {
		return nil, physical.Schema{}, fmt.Errorf("unknown table")
	}
	return table, d.schemas[name], nil
}

package slicedb

import (
	"fmt"
	"reflect"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type DatasourceExecuting struct {
	values interface{}
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	tableValue := reflect.ValueOf(d.values)
	for i := 0; i < tableValue.Len(); i++ {
		value := getValueFromReflectValue(tableValue.Index(i))

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(value.Struct, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}

var boolType = reflect.TypeOf(false)
var intType = reflect.TypeOf(int(0))
var float64Type = reflect.TypeOf(float64(0))
var stringType = reflect.TypeOf(string(""))

func getValueFromReflectValue(v reflect.Value) octosql.Value {
	switch v.Kind() {
	case reflect.Bool:
		return octosql.NewBoolean(v.Convert(boolType).Interface().(bool))
	case reflect.Int:
		return octosql.NewInt(v.Convert(intType).Interface().(int))
	case reflect.Float64:
		return octosql.NewFloat(v.Convert(float64Type).Interface().(float64))
	// case reflect.Array:
	case reflect.Ptr:
		if v.IsNil() {
			return octosql.NewNull()
		}
		return getValueFromReflectValue(v.Elem())
	case reflect.Slice:
		values := make([]octosql.Value, v.Len())
		var listType *octosql.Type
		for i := 0; i < v.Len(); i++ {
			values[i] = getValueFromReflectValue(v.Index(i))
			if listType == nil {
				listType = &values[i].Type
			} else {
				newType := octosql.TypeSum(*listType, values[i].Type)
				listType = &newType
			}
		}
		outType := octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{
				Element: listType,
			},
		}
		return octosql.Value{
			Type: outType,
			List: values,
		}
	case reflect.String:
		return octosql.NewString(v.Convert(stringType).Interface().(string))
	case reflect.Struct:
		fieldCount := v.NumField()
		fields := make([]octosql.StructField, 0, fieldCount)
		values := make([]octosql.Value, 0, fieldCount)
		for i := 0; i < fieldCount; i++ {
			if !v.Field(i).CanInterface() {
				continue
			}
			name := v.Type().Field(i).Name
			if alias, ok := v.Type().Field(i).Tag.Lookup("slicequery"); ok {
				name = alias
			}
			value := getValueFromReflectValue(v.Field(i))
			fields = append(fields, octosql.StructField{
				Name: name,
				Type: value.Type,
			})
			values = append(values, value)
		}
		outType := octosql.Type{
			TypeID: octosql.TypeIDStruct,
			Struct: struct{ Fields []octosql.StructField }{Fields: fields},
		}
		return octosql.Value{
			Type:   outType,
			Struct: values,
		}
	case reflect.Interface:
		return getValueFromReflectValue(v.Elem())
	default:
		// TODO: Test if array catches this.
		panic(fmt.Sprintf("unsupported field type: %s", v.Type()))
	}
}

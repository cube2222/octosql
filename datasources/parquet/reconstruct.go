package parquet

import (
	"fmt"

	"github.com/segmentio/parquet-go"

	"github.com/cube2222/octosql/octosql"
)

// TODO: Ewentualnie spróbuj w sumie tego podejścia bottom-up gdzie z kolumn robisz większe kolumny i wartości większe wartości, bo to by mogło zadziałać.
// Jak wiemy np. że mamy repeated contact w którym jest name i number, to robimy z tego nową kolumnę, w której mamy już struktury {name: x, number: y}
// I ta nowa kolumna też dostaje nowe repetition levele.
// Potem repeated znad tego użyje tej kolumny żeby sobie odpowiednio potworzyć listy. Itd.
// To powinno być szybkie, ale tworzy też dużo dodatkowych przejściowych kolumn, które z tym podejściem tutaj nie są potrzebne.
// Najpierw zrób to podejście tutaj bazujące na rekonstrukcji, a potem możesz spróbować zrobić podejście bottom up i porównać wydajność.
// Ważne żeby ten kod tutaj już był czysty, bez tego całego reflecta.

type levels struct {
	repetitionDepth int
	repetitionLevel int
	definitionLevel int
}

type reconstructFunc func(*octosql.Value, levels, parquet.Row) (parquet.Row, error)

func reconstructFuncOf(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	switch {
	case node.Optional():
		return reconstructFuncOfOptional(columnIndex, node)
	case node.Repeated():
		return reconstructFuncOfRepeated(columnIndex, node)
	case isList(node):
		return reconstructFuncOfList(columnIndex, node)
	// case isMap(node):
	// 	return reconstructFuncOfMap(columnIndex, node)
	default:
		return reconstructFuncOfRequired(columnIndex, node)
	}
}

func isList(node parquet.Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.List != nil
}

//go:noinline
func reconstructFuncOfOptional(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, parquet.Required(node))
	rowLength := nextColumnIndex - columnIndex
	return nextColumnIndex, func(value *octosql.Value, levels levels, row parquet.Row) (parquet.Row, error) {
		if !startsWith(row, columnIndex) {
			return row, fmt.Errorf("row is missing optional column %d", columnIndex)
		}
		if len(row) < int(rowLength) {
			return row, fmt.Errorf("expected optional column %d to have at least %d values but got %d", columnIndex, rowLength, len(row))
		}

		levels.definitionLevel++

		if row[0].DefinitionLevel() < levels.definitionLevel {
			*value = octosql.ZeroValue
			return row[rowLength:], nil
		}

		return reconstruct(value, levels, row)
	}
}

//go:noinline
func reconstructFuncOfRepeated(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, parquet.Required(node))
	rowLength := nextColumnIndex - columnIndex
	return nextColumnIndex, func(value *octosql.Value, lvls levels, row parquet.Row) (parquet.Row, error) {
		c := 10
		n := 0
		*value = octosql.Value{
			TypeID: octosql.TypeIDList,
			List:   make([]octosql.Value, c, c),
		}

		defer func() {
			value.List = value.List[:n]
		}()

		return reconstructRepeated(columnIndex, rowLength, lvls, row, func(levels levels, row parquet.Row) (parquet.Row, error) {
			if n == c {
				c *= 2
				newValue := make([]octosql.Value, c, c)
				copy(newValue, value.List)
				value.List = newValue
			}
			row, err := reconstruct(&value.List[n], levels, row)
			n++
			return row, err
		})
	}
}

func reconstructRepeated(columnIndex, rowLength int16, levels levels, row parquet.Row, do func(levels, parquet.Row) (parquet.Row, error)) (parquet.Row, error) {
	if !startsWith(row, columnIndex) {
		return row, fmt.Errorf("row is missing repeated column %d", columnIndex)
	}
	if len(row) < int(rowLength) {
		return row, fmt.Errorf("expected repeated column %d to have at least %d values but got %d", columnIndex, rowLength, len(row))
	}

	levels.repetitionDepth++
	levels.definitionLevel++

	if row[0].DefinitionLevel() < levels.definitionLevel {
		return row[rowLength:], nil
	}

	var err error
	for startsWith(row, columnIndex) && row[0].RepetitionLevel() == levels.repetitionLevel {
		if row, err = do(levels, row); err != nil {
			break
		}
		levels.repetitionLevel = levels.repetitionDepth
	}
	return row, err
}

func reconstructFuncOfRequired(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	switch {
	case node.Leaf():
		return reconstructFuncOfLeaf(columnIndex, node)
	default:
		return reconstructFuncOfGroup(columnIndex, node)
	}
}

func reconstructFuncOfList(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	return reconstructFuncOf(columnIndex, parquet.Repeated(listElementOf(node)))
}

func listElementOf(node parquet.Node) parquet.Node {
	if !node.Leaf() {
		if list := childByName(node, "list"); list != nil {
			if elem := childByName(list, "element"); elem != nil {
				return elem
			}
		}
	}
	panic("node with logical type LIST is not composed of a repeated .list.element")
}

func childByName(node parquet.Node, name string) parquet.Node {
	for _, f := range node.Fields() {
		if f.Name() == name {
			return f
		}
	}
	return nil
}

//go:noinline
// func reconstructFuncOfMap(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
// 	keyValue := mapKeyValueOf(node)
// 	keyValueType := keyValue.GoType()
// 	keyValueElem := keyValueType.Elem()
// 	keyValueZero := reflect.Zero(keyValueElem)
// 	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, schemaOf(keyValueElem))
// 	rowLength := nextColumnIndex - columnIndex
// 	return nextColumnIndex, func(mapValue *octosql.Value, lvls levels, row parquet.Row) (parquet.Row, error) {
// 		t := mapValue.Type()
// 		k := t.Key()
// 		v := t.Elem()
//
// 		if mapValue.IsNil() {
// 			mapValue.Set(reflect.MakeMap(t))
// 		}
//
// 		elem := reflect.New(keyValueElem).Elem()
// 		return reconstructRepeated(columnIndex, rowLength, lvls, row, func(levels levels, row parquet.Row) (parquet.Row, error) {
// 			row, err := reconstruct(elem, levels, row)
// 			if err == nil {
// 				mapValue.SetMapIndex(elem.Field(0).Convert(k), elem.Field(1).Convert(v))
// 				elem.Set(keyValueZero)
// 			}
// 			return row, err
// 		})
// 	}
// }

//go:noinline
func reconstructFuncOfGroup(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	fields := node.Fields()
	funcs := make([]reconstructFunc, len(fields))
	columnIndexes := make([]int16, len(fields))

	for i, field := range fields {
		columnIndex, funcs[i] = reconstructFuncOf(columnIndex, field)
		columnIndexes[i] = columnIndex
	}

	return columnIndex, func(value *octosql.Value, levels levels, row parquet.Row) (parquet.Row, error) {
		var err error

		*value = octosql.Value{
			TypeID: octosql.TypeIDStruct,
			Struct: make([]octosql.Value, len(fields)),
		}

		for i, f := range funcs {
			if row, err = f(&value.Struct[i], levels, row); err != nil {
				err = fmt.Errorf("%s → %w", fields[i].Name(), err)
				break
			}
		}

		return row, err
	}
}

//go:noinline
func reconstructFuncOfLeaf(columnIndex int16, node parquet.Node) (int16, reconstructFunc) {
	return columnIndex + 1, func(value *octosql.Value, _ levels, row parquet.Row) (parquet.Row, error) {
		if !startsWith(row, columnIndex) {
			return row, fmt.Errorf("no values found in parquet row for column %d", columnIndex)
		}
		return row[1:], assignValue(value, row[0])
	}
}

func startsWith(row parquet.Row, columnIndex int16) bool {
	return len(row) > 0 && row[0].Column() == int(columnIndex)
}

func assignValue(dst *octosql.Value, src parquet.Value) error {
	if src.IsNull() {
		*dst = octosql.ZeroValue
		return nil
	}

	switch src.Kind() {
	case parquet.Boolean:
		*dst = octosql.NewBoolean(src.Boolean())
	case parquet.Int32:
		*dst = octosql.NewInt(int(src.Int32()))
	case parquet.Int64:
		*dst = octosql.NewInt(int(src.Int64()))
	case parquet.Float:
		*dst = octosql.NewFloat(float64(src.Float()))
	case parquet.Double:
		*dst = octosql.NewFloat(src.Double())
	case parquet.ByteArray:
		*dst = octosql.NewString(string(src.ByteArray())) // TODO: Fixme handle bytes.
	case parquet.FixedLenByteArray:
		*dst = octosql.NewString(string(src.ByteArray())) // TODO: Fixme handle bytes.
	default:
		*dst = octosql.ZeroValue
	}
	return nil
}

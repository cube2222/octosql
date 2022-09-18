// This file is based on the reconstructor code in "github.com/segmentio/parquet-go".
//
//   Copyright 2022 segment.io
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
//   ----------------------------------------------------------------------------
//
//   This Source Code Form is subject to the terms of the Mozilla Public
//   License, v. 2.0. If a copy of the MPL was not distributed with this
//   file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package parquet

import (
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/segmentio/parquet-go"

	"github.com/cube2222/octosql/codegen"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type codegenReconstructFunc string

func codegenReconstructFuncOfSchemaFields(g *codegen.CodeGenerator, rowVariable string, schema physical.Schema, node parquet.Node, usedFieldNames []string) []codegen.RecordField {
	var columnIndex int16 = 0
	fields := node.Fields()

	outRecordFields := make([]codegen.RecordField, 0, len(usedFieldNames))
	i := 0
	for _, field := range fields {
		var curFunc codegenReconstructFunc
		if slices.Contains(usedFieldNames, field.Name()) {
			register := g.DeclareVariable(schema.Fields[i].Name, schema.Fields[i].Type)
			outRecordFields = append(outRecordFields, codegen.RecordField{
				Name:  schema.Fields[i].Name,
				Value: register,
			})

			columnIndex, curFunc = codegenReconstructFuncOf(columnIndex, rowVariable, register, field)
			g.Body().WriteString(string(curFunc))
			i++
		} else {
			columnIndex, curFunc = codegenReconstructFuncOf(columnIndex, rowVariable, codegen.Register{}, field)
		}
	}

	return outRecordFields
}

func codegenReconstructFuncOf(columnIndex int16, rowVariable string, register codegen.Register, node parquet.Node) (int16, codegenReconstructFunc) {
	switch {
	// case node.Optional():
	// 	return codegenReconstructFuncOfOptional(columnIndex, rowVariable, register, node)
	// case node.Repeated():
	// 	return codegenReconstructFuncOfRepeated(columnIndex, rowVariable, register, node)
	// case isList(node):
	// 	return codegenReconstructFuncOfList(columnIndex, node)
	// case isMap(node):
	// 	return codegenReconstructFuncOfMap(columnIndex, node)
	default:
		return codegenReconstructFuncOfRequired(columnIndex, rowVariable, register, node)
	}
}

// func isList(node parquet.Node) bool {
// 	logicalType := node.Type().LogicalType()
// 	return logicalType != nil && logicalType.List != nil
// }

//go:noinline
// func codegenReconstructFuncOfOptional(columnIndex int16, rowRegister, register codegen.Register, node parquet.Node) (int16, codegenReconstructFunc) {
// 	nextColumnIndex, reconstruct := codegenReconstructFuncOf(columnIndex, parquet.Required(node))
// 	rowLength := nextColumnIndex - columnIndex
// 	return nextColumnIndex, func(value *octosql.Value, levels levels, row parquet.Row) (parquet.Row, error) {
// 		if !startsWith(row, columnIndex) {
// 			return row, fmt.Errorf("row is missing optional column %d", columnIndex)
// 		}
// 		if len(row) < int(rowLength) {
// 			return row, fmt.Errorf("expected optional column %d to have at least %d values but got %d", columnIndex, rowLength, len(row))
// 		}
//
// 		levels.definitionLevel++
//
// 		if row[0].DefinitionLevel() < levels.definitionLevel {
// 			*value = octosql.ZeroValue
// 			return row[rowLength:], nil
// 		}
//
// 		return reconstruct(value, levels, row)
// 	}
// }

//go:noinline
// func codegenReconstructFuncOfRepeated(columnIndex int16, rowRegister, register codegen.Register, node parquet.Node) (int16, codegenReconstructFunc) {
// 	nextColumnIndex, reconstruct := codegenReconstructFuncOf(columnIndex, parquet.Required(node))
// 	rowLength := nextColumnIndex - columnIndex
// 	return nextColumnIndex, func(value *octosql.Value, lvls levels, row parquet.Row) (parquet.Row, error) {
// 		c := 10
// 		n := 0
// 		*value = octosql.Value{
// 			TypeID: octosql.TypeIDList,
// 			List:   make([]octosql.Value, c, c),
// 		}
//
// 		defer func() {
// 			value.List = value.List[:n]
// 		}()
//
// 		return reconstructRepeated(columnIndex, rowLength, lvls, row, func(levels levels, row parquet.Row) (parquet.Row, error) {
// 			if n == c {
// 				c *= 2
// 				newValue := make([]octosql.Value, c, c)
// 				copy(newValue, value.List)
// 				value.List = newValue
// 			}
// 			row, err := reconstruct(&value.List[n], levels, row)
// 			n++
// 			return row, err
// 		})
// 	}
// }
//
// func reconstructRepeated(columnIndex, rowLength int16, levels levels, row parquet.Row, do func(levels, parquet.Row) (parquet.Row, error)) (parquet.Row, error) {
// 	if !startsWith(row, columnIndex) {
// 		return row, fmt.Errorf("row is missing repeated column %d", columnIndex)
// 	}
// 	if len(row) < int(rowLength) {
// 		return row, fmt.Errorf("expected repeated column %d to have at least %d values but got %d", columnIndex, rowLength, len(row))
// 	}
//
// 	levels.repetitionDepth++
// 	levels.definitionLevel++
//
// 	if row[0].DefinitionLevel() < levels.definitionLevel {
// 		return row[rowLength:], nil
// 	}
//
// 	var err error
// 	for startsWith(row, columnIndex) && row[0].RepetitionLevel() == levels.repetitionLevel {
// 		if row, err = do(levels, row); err != nil {
// 			break
// 		}
// 		levels.repetitionLevel = levels.repetitionDepth
// 	}
// 	return row, err
// }

func codegenReconstructFuncOfRequired(columnIndex int16, rowVariable string, register codegen.Register, node parquet.Node) (int16, codegenReconstructFunc) {
	switch {
	case node.Leaf():
		return codegenReconstructFuncOfLeaf(columnIndex, rowVariable, register, node)
	default:
		// return codegenReconstructFuncOfGroup(columnIndex, node)
		panic("implement me")
	}
}

// func codegenReconstructFuncOfList(columnIndex int16, node parquet.Node) (int16, codegenReconstructFunc) {
// 	return codegenReconstructFuncOf(columnIndex, parquet.Repeated(listElementOf(node)))
// }
//
// func listElementOf(node parquet.Node) parquet.Node {
// 	if !node.Leaf() {
// 		if list := childByName(node, "list"); list != nil {
// 			if elem := childByName(list, "element"); elem != nil {
// 				return elem
// 			}
// 		}
// 	}
// 	panic("node with logical type LIST is not composed of a repeated .list.element")
// }
//
// func childByName(node parquet.Node, name string) parquet.Node {
// 	for _, f := range node.Fields() {
// 		if f.Name() == name {
// 			return f
// 		}
// 	}
// 	return nil
// }

//go:noinline
// func codegenReconstructFuncOfMap(columnIndex int16, node parquet.Node) (int16, codegenReconstructFunc) {
// 	keyValue := mapKeyValueOf(node)
// 	keyValueType := keyValue.GoType()
// 	keyValueElem := keyValueType.Elem()
// 	keyValueZero := reflect.Zero(keyValueElem)
// 	nextColumnIndex, reconstruct := codegenReconstructFuncOf(columnIndex, schemaOf(keyValueElem))
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
// func codegenReconstructFuncOfGroup(columnIndex int16, node parquet.Node) (int16, codegenReconstructFunc) {
// 	fields := node.Fields()
// 	funcs := make([]codegenReconstructFunc, 0, len(fields))
// 	columnIndexes := make([]int16, 0, len(fields))
//
// 	for _, field := range fields {
// 		var curFunc codegenReconstructFunc
// 		columnIndex, curFunc = codegenReconstructFuncOf(columnIndex, field)
// 		if field.Name() != "rate_code_id" {
// 			columnIndexes = append(columnIndexes, columnIndex)
// 			funcs = append(funcs, curFunc)
// 		}
// 	}
//
// 	return columnIndex, func(value *octosql.Value, levels levels, row parquet.Row) (parquet.Row, error) {
// 		var err error
//
// 		*value = octosql.Value{
// 			TypeID: octosql.TypeIDStruct,
// 			Struct: make([]octosql.Value, len(fields)),
// 		}
//
// 		for i, f := range funcs {
// 			if row, err = f(&value.Struct[i], levels, row); err != nil {
// 				err = fmt.Errorf("%s → %w", fields[i].Name(), err)
// 				break
// 			}
// 		}
//
// 		return row, err
// 	}
// }

//go:noinline
func codegenReconstructFuncOfLeaf(columnIndex int16, rowVariable string, register codegen.Register, node parquet.Node) (int16, codegenReconstructFunc) {
	g := codegen.NewGenerator()
	g.PrintflnAdvanced(`
if !(len($row) > 0 && $row[0].Column() == int($columnIndex)) {
  panic("no values found in parquet row for column $columnIndex")
}
`, map[string]string{
		"row":         rowVariable,
		"columnIndex": fmt.Sprintf("%d", columnIndex),
	})

	codegenAssignValue(g, rowVariable, register, node)
	g.Printfln("%s = %s[1:]", rowVariable, rowVariable)
	return columnIndex + 1, codegenReconstructFunc(g.Body().String())
}

func codegenAssignValue(g *codegen.CodeGenerator, rowVariable string, register codegen.Register, src parquet.Node) error {
	// 	mapping := map[string]string{
	// 		"row": rowVariable,
	// 	}
	//
	// 	if src.Optional() {
	// 		g.PrintflnAdvanced(`
	// if $row[0].IsNull() {
	// `, mapping)
	// 		g.SetVariable(register, octosql.TypeIDNull, "struct{}")
	// 		g.Printfln(`
	// continue
	// }
	// `)
	// 	}

	switch src.Type().Kind() {
	case parquet.Boolean:
		g.SetVariable(register, octosql.TypeIDBoolean, fmt.Sprintf("%s[0].Boolean()", rowVariable))
	case parquet.Int32:
		g.SetVariable(register, octosql.TypeIDInt, fmt.Sprintf("int(%s[0].Int32())", rowVariable))
	case parquet.Int64:
		g.SetVariable(register, octosql.TypeIDInt, fmt.Sprintf("int(%s[0].Int64())", rowVariable))
	// case parquet.Int96:
	// 	*dst = octosql.NewString(src.Int96().String())
	case parquet.Float:
		g.SetVariable(register, octosql.TypeIDFloat, fmt.Sprintf("float64(%s[0].Float())", rowVariable))
	case parquet.Double:
		g.SetVariable(register, octosql.TypeIDFloat, fmt.Sprintf("float64(%s[0].Double())", rowVariable))
	case parquet.ByteArray:
		g.SetVariable(register, octosql.TypeIDString, fmt.Sprintf("string(%s[0].ByteArray())", rowVariable)) // TODO: Fixme handle bytes.
	case parquet.FixedLenByteArray:
		g.SetVariable(register, octosql.TypeIDString, fmt.Sprintf("string(%s[0].ByteArray())", rowVariable)) // TODO: Fixme handle bytes.
		// default:
		// 	*dst = octosql.ZeroValue
	}
	return nil
}

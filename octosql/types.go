package octosql

import (
	"fmt"
	"sort"
	"strings"
)

type TypeID int

const (
	TypeIDNull TypeID = iota
	TypeIDInt
	TypeIDFloat
	TypeIDBoolean
	TypeIDString
	TypeIDTime
	TypeIDDuration
	TypeIDList
	TypeIDStruct
	TypeIDTuple
	TypeIDUnion
	TypeIDAny // TODO: Remove this type?
)

type Type struct {
	TypeID   TypeID
	Null     struct{}
	Int      struct{}
	Float    struct{}
	Boolean  struct{}
	Str      struct{}
	Time     struct{}
	Duration struct{}
	List     struct {
		Element *Type
	}
	Struct struct {
		Fields []StructField // TODO: -> Names []string, Types []Type
	}
	Tuple struct {
		Elements []Type
	}
	Union struct {
		Alternatives []Type
	}
	Any struct{}
}

type StructField struct {
	Name string
	Type Type
}

type TypeRelation int

const (
	TypeRelationIsnt TypeRelation = iota
	TypeRelationMaybe
	TypeRelationIs
)

func (t Type) Equals(other Type) bool {
	return t.Is(other) == TypeRelationIs && other.Is(t) == TypeRelationIs
}

func (t Type) Is(other Type) TypeRelation {
	if other.TypeID == TypeIDAny {
		return TypeRelationIs
	}
	if t.TypeID == TypeIDUnion {
		anyFits := false
		allFit := true
		for _, alternative := range t.Union.Alternatives {
			rel := alternative.Is(other)
			if rel == TypeRelationIs {
				anyFits = true
			} else if rel == TypeRelationMaybe {
				anyFits = true
				allFit = false
			} else {
				allFit = false
			}
		}
		if allFit {
			return TypeRelationIs
		} else if anyFits {
			return TypeRelationMaybe
		} else {
			return TypeRelationIsnt
		}
	}
	if other.TypeID == TypeIDUnion {
		out := TypeRelationIsnt
		for _, alternative := range other.Union.Alternatives {
			rel := t.Is(alternative)
			if rel > out {
				out = rel
			}
		}
		return out
	}
	if t.TypeID == TypeIDList {
		if other.TypeID != TypeIDList {
			return TypeRelationIsnt
		}
		if other.List.Element == nil && t.List.Element != nil ||
			t.List.Element != nil && t.List.Element.Is(*other.List.Element) < TypeRelationIs {
			return TypeRelationIsnt
		}
		return TypeRelationIs
	}
	if t.TypeID == TypeIDStruct {
		if other.TypeID != TypeIDStruct {
			return TypeRelationIsnt
		}
		if len(t.Struct.Fields) != len(other.Struct.Fields) {
			return TypeRelationIsnt
		}
		for i := range t.Struct.Fields {
			if t.Struct.Fields[i].Name != other.Struct.Fields[i].Name {
				return TypeRelationIsnt
			}
			if t.Struct.Fields[i].Type.Is(other.Struct.Fields[i].Type) < TypeRelationIs {
				return TypeRelationIsnt // TODO: Why is this isn't, and not maybe?
			}
		}
		return TypeRelationIs
	}
	if t.TypeID == TypeIDTuple {
		if other.TypeID != TypeIDTuple {
			return TypeRelationIsnt
		}
		if len(t.Tuple.Elements) != len(other.Tuple.Elements) {
			return TypeRelationIsnt
		}
		for i := range t.Tuple.Elements {
			if t.Tuple.Elements[i].Is(other.Tuple.Elements[i]) < TypeRelationIs {
				return TypeRelationIsnt
			}
		}
		return TypeRelationIs
	}
	if t.TypeID == other.TypeID {
		return TypeRelationIs
	}
	return TypeRelationIsnt
}

func (t Type) String() string {
	switch t.TypeID {
	case TypeIDNull:
		return "NULL"
	case TypeIDInt:
		return "Int"
	case TypeIDFloat:
		return "Float"
	case TypeIDBoolean:
		return "Boolean"
	case TypeIDString:
		return "String"
	case TypeIDTime:
		return "Time"
	case TypeIDDuration:
		return "Duration"
	case TypeIDList:
		if t.List.Element == nil {
			return "[]"
		}
		return fmt.Sprintf("[%s]", *t.List.Element)
	case TypeIDStruct:
		fieldStrings := make([]string, len(t.Struct.Fields))
		for i, field := range t.Struct.Fields {
			fieldStrings[i] = fmt.Sprintf("%s: %s", field.Name, field.Type)
		}

		return fmt.Sprintf("{%s}", strings.Join(fieldStrings, "; "))
	case TypeIDTuple:
		typeStrings := make([]string, len(t.Tuple.Elements))
		for i, element := range t.Tuple.Elements {
			typeStrings[i] = element.String()
		}

		return fmt.Sprintf("(%s)", strings.Join(typeStrings, ", "))
	case TypeIDUnion:
		typeStrings := make([]string, len(t.Union.Alternatives))
		for i, alternative := range t.Union.Alternatives {
			typeStrings[i] = alternative.String()
		}

		return strings.Join(typeStrings, " | ")
	case TypeIDAny:
		return "Any"
	}
	panic("impossible, type switch bug")
}

var (
	Null     = Type{TypeID: TypeIDNull}
	Int      = Type{TypeID: TypeIDInt}
	Float    = Type{TypeID: TypeIDFloat}
	Boolean  = Type{TypeID: TypeIDBoolean}
	String   = Type{TypeID: TypeIDString}
	Time     = Type{TypeID: TypeIDTime}
	Duration = Type{TypeID: TypeIDDuration}
	Any      = Type{TypeID: TypeIDAny}
)

func TypeSum(t1, t2 Type) Type {
	// TODO: For field access, field.* would be nice, to get all fields out of a structure.
	if t1.Is(t2) == TypeRelationIs {
		return t2
	}
	if t2.Is(t1) == TypeRelationIs {
		return t1
	}
	// TODO: Lists should also be deep-merged.
	if t1.TypeID == TypeIDStruct && t2.TypeID == TypeIDStruct {
		// TODO: Nullable struct + Nullable struct should also work. Overall, it should try to match if there are multiple struct alternatives.
		// The sum of structs is a struct that's a deep merge of the original structs.

		t1Fields := make(map[string]Type)
		for i := range t1.Struct.Fields {
			t1Fields[t1.Struct.Fields[i].Name] = t1.Struct.Fields[i].Type
		}
		t2Fields := make(map[string]Type)
		for i := range t2.Struct.Fields {
			t2Fields[t2.Struct.Fields[i].Name] = t2.Struct.Fields[i].Type
		}
		outFields := make(map[string]Type)
		for name := range t1Fields {
			if _, ok := t2Fields[name]; ok {
				outFields[name] = TypeSum(t1Fields[name], t2Fields[name])
			} else {
				outFields[name] = TypeSum(t1Fields[name], Null)
			}
		}
		for name := range t2Fields {
			if _, ok := t1Fields[name]; !ok {
				outFields[name] = TypeSum(t2Fields[name], Null)
			}
		}

		outFieldSlice := make([]StructField, 0, len(outFields))
		for name, fieldType := range outFields {
			outFieldSlice = append(outFieldSlice, StructField{Name: name, Type: fieldType})
		}
		sort.Slice(outFieldSlice, func(i, j int) bool {
			return outFieldSlice[i].Name < outFieldSlice[j].Name
		})

		return Type{
			TypeID: TypeIDStruct,
			Struct: struct {
				Fields []StructField
			}{
				Fields: outFieldSlice,
			},
		}
	}
	// TODO: This won't work well with structs. If we have a union and we're adding a struct, try to find an already existing struct.
	// TODO: If we deep-merge both lists and structs, the union should not be a list of alternatives, because each TypeID can only be there once.
	var alternatives []Type
	addType := func(t Type) {
		if t.Is(Type{
			TypeID: TypeIDUnion,
			Union:  struct{ Alternatives []Type }{Alternatives: alternatives},
		}) != TypeRelationIs {
			alternatives = append(alternatives, t)
		}
	}
	if t1.TypeID != TypeIDUnion {
		addType(t1)
	} else {
		for _, alternative := range t1.Union.Alternatives {
			addType(alternative)
		}
	}
	if t2.TypeID != TypeIDUnion {
		addType(t2)
	} else {
		for _, alternative := range t2.Union.Alternatives {
			addType(alternative)
		}
	}
	// This could be even more normalized, by removing all alternative elements for which:
	// alternatives[i].Is(Union{alternatives[..i] ++ alternatives[i+1..]})
	if len(alternatives) == 1 {
		return alternatives[0]
	}
	return Type{
		TypeID: TypeIDUnion,
		Union:  struct{ Alternatives []Type }{Alternatives: alternatives},
	}
}

func TypeIntersection(t1, t2 Type) *Type {
	leftPossibleTypes := t1.possiblePrimitiveTypes()
	rightPossibleTypes := t2.possiblePrimitiveTypes()

	var outputType *Type
	for _, t := range leftPossibleTypes {
		if t.Is(t2) == TypeRelationIs {
			if outputType == nil {
				outputType = &t
			} else {
				*outputType = TypeSum(*outputType, t)
			}
		}
	}
	for _, t := range rightPossibleTypes {
		if t.Is(t1) == TypeRelationIs {
			if outputType == nil {
				outputType = &t
			} else {
				*outputType = TypeSum(*outputType, t)
			}
		}
	}
	return outputType
}

func (t Type) possiblePrimitiveTypes() []Type {
	switch t.TypeID {
	case TypeIDUnion:
		out := make([]Type, 0, len(t.Union.Alternatives))
		for i := range t.Union.Alternatives {
			out = append(out, t.Union.Alternatives[i].possiblePrimitiveTypes()...)
		}
		return out
	default:
		return []Type{t}
	}
}

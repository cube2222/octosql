package octosql

import (
	"fmt"
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
	TypeIDAny
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

func (t Type) Is(other Type) TypeRelation {
	// TODO: Implement me
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
	// TODO: Lists should probably be the same. No, we can actually easily check lists at runtime by checking their actual element types. Empty list fits into anything.
	if t1.TypeID == TypeIDStruct && t2.TypeID == TypeIDStruct {
		// TODO: Nullable struct + Nullable struct should also work. Overall, it should try to match if there are multiple struct alternatives.
		// Sum of structs is a struct. We operate on the fields.
		// This only works if all non-overlapping fields are nullable.
		outFields := make([]StructField, len(t1.Struct.Fields))
		copy(outFields, t1.Struct.Fields)
		matchedFields := make([]bool, len(t1.Struct.Fields))

	newFieldLoop:
		for _, field := range t2.Struct.Fields {
			for i, existing := range outFields {
				if existing.Name == field.Name {
					outFields[i].Type = TypeSum(existing.Type, field.Type)
					matchedFields[i] = true
					continue newFieldLoop
				}
			}
			outFields = append(outFields, field)
			matchedFields = append(matchedFields, false)
		}

		nonNullableFieldsMatch := true
		for i := range outFields {
			if !matchedFields[i] && Null.Is(outFields[i].Type) < TypeRelationIs {
				nonNullableFieldsMatch = false
			}
		}

		if nonNullableFieldsMatch {
			return Type{
				TypeID: TypeIDStruct,
				Struct: struct {
					Fields []StructField
				}{
					Fields: outFields,
				},
			}
		}
	}
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

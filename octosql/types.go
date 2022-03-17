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

func (t TypeID) String() string {
	switch t {
	case TypeIDNull:
		return "Null"
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
		return "List"
	case TypeIDStruct:
		return "Object"
	case TypeIDTuple:
		return "Tuple"
	case TypeIDUnion:
		return "Union"
	case TypeIDAny:
		return "Any"
	}
	return "Invalid"
}

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
	if t1.TypeID == TypeIDStruct && t2.TypeID == TypeIDStruct {
		// Deep merge structs.

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
	if t1.TypeID == TypeIDList && t2.TypeID == TypeIDList {
		// Deep merge lists.

		if t1.List.Element == nil {
			return t2
		}
		if t2.List.Element == nil {
			return t1
		}
		sum := TypeSum(*t1.List.Element, *t2.List.Element)
		return Type{
			TypeID: TypeIDList,
			List: struct {
				Element *Type
			}{
				Element: &sum,
			},
		}
	}
	if t1.TypeID == TypeIDTuple && t2.TypeID == TypeIDTuple {
		// Deep merge tuples.

		var longer, shorter []Type
		if len(t1.Tuple.Elements) > len(t2.Tuple.Elements) {
			longer = t1.Tuple.Elements
			shorter = t2.Tuple.Elements
		} else {
			longer = t2.Tuple.Elements
			shorter = t1.Tuple.Elements
		}

		elements := make([]Type, len(longer))
		copy(elements, longer)
		for i := range shorter {
			elements[i] = TypeSum(elements[i], shorter[i])
		}
		for i := len(shorter); i < len(longer); i++ {
			elements[i] = TypeSum(elements[i], Null)
		}

		return Type{
			TypeID: TypeIDTuple,
			Tuple:  struct{ Elements []Type }{Elements: elements},
		}
	}
	if t1.TypeID == TypeIDUnion && t2.TypeID == TypeIDUnion {
		out := Type{
			TypeID: TypeIDUnion,
			Union:  struct{ Alternatives []Type }{Alternatives: t1.Union.Alternatives},
		}
		for _, alternative := range t2.Union.Alternatives {
			out = TypeSum(out, alternative)
		}
		return out
	}
	// If only one is a union, t1 shall be (for simplicity).
	if t2.TypeID == TypeIDUnion {
		return TypeSum(t2, t1)
	}
	if t1.TypeID == TypeIDUnion {
		alternatives := make([]Type, len(t1.Union.Alternatives))
		copy(alternatives, t1.Union.Alternatives)
		for i := range alternatives {
			// We only want each TypeID once in the union.
			if alternatives[i].TypeID == t2.TypeID {
				alternatives[i] = TypeSum(alternatives[i], t2)
				return Type{
					TypeID: TypeIDUnion,
					Union:  struct{ Alternatives []Type }{Alternatives: alternatives},
				}
			}
		}
		// If this TypeID is not yet here, append and normalize.
		alternatives = append(alternatives, t2)
		sort.Slice(alternatives, func(i, j int) bool {
			return alternatives[i].TypeID < alternatives[j].TypeID
		})
		return Type{
			TypeID: TypeIDUnion,
			Union:  struct{ Alternatives []Type }{Alternatives: alternatives},
		}
	}

	// They are different TypeID's and none of them is a union. Return a union of them.
	alternatives := []Type{t1, t2}
	sort.Slice(alternatives, func(i, j int) bool {
		return alternatives[i].TypeID < alternatives[j].TypeID
	})

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

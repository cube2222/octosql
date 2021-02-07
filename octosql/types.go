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
		Fields []StructField
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
		if t.List.Element.Is(*other.List.Element) < TypeRelationIs {
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
	Null     Type = Type{TypeID: TypeIDNull}
	Int      Type = Type{TypeID: TypeIDInt}
	Float    Type = Type{TypeID: TypeIDFloat}
	Boolean  Type = Type{TypeID: TypeIDBoolean}
	String   Type = Type{TypeID: TypeIDString}
	Time     Type = Type{TypeID: TypeIDTime}
	Duration Type = Type{TypeID: TypeIDDuration}
	Any      Type = Type{TypeID: TypeIDAny}
)

func TypeSum(t1, t2 Type) Type {
	if t1.Is(t2) == TypeRelationIs {
		return t2
	}
	if t2.Is(t1) == TypeRelationIs {
		return t1
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

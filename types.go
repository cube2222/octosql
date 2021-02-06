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
}

type StructField struct {
	Name string
	Type Type
}

type TypeRelation int

const (
	TypeRelationIs TypeRelation = iota
	TypeRelationMaybe
	TypeRelationIsnt
)

func (t Type) Is(other Type) TypeRelation {
	// TODO: Implement me
	return TypeRelationIs
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

		return fmt.Sprintf("Union<%s>", strings.Join(typeStrings, ", "))
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
)

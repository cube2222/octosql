package plugins

import "github.com/cube2222/octosql/octosql"

func TypeToProto(t octosql.Type) *Type {
	switch t.TypeID {
	case octosql.TypeIDNull:
		return &Type{Type: &Type_Null{Null: &TypeNull{}}}
	case octosql.TypeIDInt:
		return &Type{Type: &Type_Int{Int: &TypeInt{}}}
	case octosql.TypeIDFloat:
		return &Type{Type: &Type_Float{Float: &TypeFloat{}}}
	case octosql.TypeIDBoolean:
		return &Type{Type: &Type_Boolean{Boolean: &TypeBoolean{}}}
	case octosql.TypeIDString:
		return &Type{Type: &Type_Str{Str: &TypeStr{}}}
	case octosql.TypeIDTime:
		return &Type{Type: &Type_Time{Time: &TypeTime{}}}
	case octosql.TypeIDDuration:
		return &Type{Type: &Type_Duration{Duration: &TypeDuration{}}}
	case octosql.TypeIDList:
		return &Type{Type: &Type_List{List: &TypeList{Element: TypeToProto(*t.List.Element)}}}
	case octosql.TypeIDStruct:
		fields := make([]*TypeStructField, len(t.Struct.Fields))
		for i := range t.Struct.Fields {
			fields[i] = &TypeStructField{
				Name: t.Struct.Fields[i].Name,
				Type: TypeToProto(t.Struct.Fields[i].Type),
			}
		}
		return &Type{Type: &Type_Struct{Struct: &TypeStruct{Fields: fields}}}
	case octosql.TypeIDUnion:
		alternatives := make([]*Type, len(t.Union.Alternatives))
		for i := range t.Union.Alternatives {
			alternatives[i] = TypeToProto(t.Union.Alternatives[i])
		}
		return &Type{Type: &Type_Union{Union: &TypeUnion{Alternatives: alternatives}}}
	case octosql.TypeIDAny:
		return &Type{Type: &Type_Any{Any: &TypeAny{}}}
	}

	panic("unexhaustive type match")
}

func TypeFromProto(t *Type) octosql.Type {
	switch typedata := t.Type.(type) {
	case *Type_Null:
		return octosql.Null
	case *Type_Int:
		return octosql.Int
	case *Type_Float:
		return octosql.Float
	case *Type_Boolean:
		return octosql.Boolean
	case *Type_Str:
		return octosql.String
	case *Type_Time:
		return octosql.Time
	case *Type_Duration:
		return octosql.Duration
	case *Type_List:
		element := TypeFromProto(typedata.List.Element)
		return octosql.Type{TypeID: octosql.TypeIDList, List: struct{ Element *octosql.Type }{Element: &element}}
	case *Type_Struct:
		fields := make([]octosql.StructField, len(typedata.Struct.Fields))
		for i := range typedata.Struct.Fields {
			fields[i] = octosql.StructField{
				Name: typedata.Struct.Fields[i].Name,
				Type: TypeFromProto(typedata.Struct.Fields[i].Type),
			}
		}
		return octosql.Type{TypeID: octosql.TypeIDStruct, Struct: struct{ Fields []octosql.StructField }{Fields: fields}}
	case *Type_Union:
		alternatives := make([]octosql.Type, len(typedata.Union.Alternatives))
		for i := range typedata.Union.Alternatives {
			alternatives[i] = TypeFromProto(typedata.Union.Alternatives[i])
		}
		return octosql.Type{TypeID: octosql.TypeIDStruct, Union: struct{ Alternatives []octosql.Type }{Alternatives: alternatives}}
	case *Type_Any:
		return octosql.Any
	}

	panic("unexhaustive type match")
}

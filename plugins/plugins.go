package plugins

import (
	"fmt"

	"github.com/gogo/protobuf/types"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

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

func ValueToProto(v octosql.Value) *Value {
	switch v.Type.TypeID {
	case octosql.TypeIDNull:
		return &Value{Type: TypeToProto(v.Type), Value: &Value_Null{Null: true}}
	case octosql.TypeIDInt:
		return &Value{Type: TypeToProto(v.Type), Value: &Value_Int{Int: int64(v.Int)}}
	case octosql.TypeIDFloat:
		return &Value{Type: TypeToProto(v.Type), Value: &Value_Float{Float: v.Float}}
	case octosql.TypeIDBoolean:
		return &Value{Type: TypeToProto(v.Type), Value: &Value_Boolean{Boolean: v.Boolean}}
	case octosql.TypeIDString:
		return &Value{Type: TypeToProto(v.Type), Value: &Value_Str{Str: v.Str}}
	case octosql.TypeIDTime:
		ts, err := types.TimestampProto(v.Time)
		if err != nil {
			panic(fmt.Errorf("couldn't convert time to proto timestamp"))
		}

		return &Value{Type: TypeToProto(v.Type), Value: &Value_Time{Time: ts}}
	case octosql.TypeIDDuration:
		return &Value{Type: TypeToProto(v.Type), Value: &Value_Duration{Duration: types.DurationProto(v.Duration)}}
	case octosql.TypeIDList:
		values := make([]*Value, len(v.List))
		for i := range v.List {
			values[i] = ValueToProto(v.List[i])
		}
		return &Value{Type: TypeToProto(v.Type), Value: &Value_List{List: &Values{Values: values}}}
	case octosql.TypeIDStruct:
		values := make([]*Value, len(v.FieldValues))
		for i := range v.FieldValues {
			values[i] = ValueToProto(v.FieldValues[i])
		}
		return &Value{Type: TypeToProto(v.Type), Value: &Value_FieldValues{FieldValues: &Values{Values: values}}}
	}

	panic("unexhaustive type match")
}

func ValueFromProto(v *Value) octosql.Value {
	switch value := v.Value.(type) {
	case *Value_Null:
		return octosql.NewNull()
	case *Value_Int:
		return octosql.NewInt(int(value.Int))
	case *Value_Float:
		return octosql.NewFloat(value.Float)
	case *Value_Boolean:
		return octosql.NewBoolean(value.Boolean)
	case *Value_Str:
		return octosql.NewString(value.Str)
	case *Value_Time:
		ts, err := types.TimestampFromProto(value.Time)
		if err != nil {
			panic(fmt.Errorf("couldn't convert proto timestamp to time"))
		}
		return octosql.NewTime(ts)
	case *Value_Duration:
		d, err := types.DurationFromProto(value.Duration)
		if err != nil {
			panic(fmt.Errorf("couldn't convert proto duration to duration"))
		}
		return octosql.NewDuration(d)
	case *Value_List:
		values := make([]octosql.Value, len(value.List.Values))
		for i := range value.List.Values {
			values[i] = ValueFromProto(value.List.Values[i])
		}
		return octosql.NewList(values)
	case *Value_FieldValues:
		values := make([]octosql.Value, len(value.FieldValues.Values))
		for i := range value.FieldValues.Values {
			values[i] = ValueFromProto(value.FieldValues.Values[i])
		}
		return octosql.NewFieldValues(values)
	}

	panic("unexhaustive type match")
}

func ExpressionToProto(expr physical.Expression) *Expression {
	switch expr.ExpressionType {
	case physical.ExpressionTypeVariable:
		return &Expression{
			Type: TypeToProto(expr.Type),
			Expression: &Expression_Variable{
				Variable: &Variable{
					Name:      expr.Variable.Name,
					IsLevel_0: expr.Variable.IsLevel0,
				},
			},
		}
	case physical.ExpressionTypeConstant:
		return &Expression{
			Type: TypeToProto(expr.Type),
			Expression: &Expression_Constant{
				Constant: &Constant{
					Value: ValueToProto(expr.Constant.Value),
				},
			},
		}
	case physical.ExpressionTypeFunctionCall:
		arguments := make([]*Expression, len(expr.FunctionCall.Arguments))
		for i := range expr.FunctionCall.Arguments {
			arguments[i] = ExpressionToProto(expr.FunctionCall.Arguments[i])
		}
		return &Expression{
			Type: TypeToProto(expr.Type),
			Expression: &Expression_FunctionCall{
				FunctionCall: &FunctionCall{
					Name:      expr.FunctionCall.Name,
					Arguments: arguments,
				},
			},
		}
	case physical.ExpressionTypeAnd:
		arguments := make([]*Expression, len(expr.And.Arguments))
		for i := range expr.And.Arguments {
			arguments[i] = ExpressionToProto(expr.And.Arguments[i])
		}
		return &Expression{
			Type: TypeToProto(expr.Type),
			Expression: &Expression_And{
				And: &And{
					Arguments: arguments,
				},
			},
		}
	case physical.ExpressionTypeOr:
		arguments := make([]*Expression, len(expr.Or.Arguments))
		for i := range expr.Or.Arguments {
			arguments[i] = ExpressionToProto(expr.Or.Arguments[i])
		}
		return &Expression{
			Type: TypeToProto(expr.Type),
			Expression: &Expression_Or{
				Or: &Or{
					Arguments: arguments,
				},
			},
		}
	case physical.ExpressionTypeTypeAssertion:
		return &Expression{
			Type: TypeToProto(expr.Type),
			Expression: &Expression_TypeAssertion{
				TypeAssertion: &TypeAssertion{
					Expression: ExpressionToProto(expr.TypeAssertion.Expression),
					TargetType: TypeToProto(expr.TypeAssertion.TargetType),
				},
			},
		}
	}

	panic("unexhaustive expression type match")
}

func ExpressionFromProto(expr *Expression) physical.Expression {
	switch typed := expr.Expression.(type) {
	case *Expression_Variable:
		return physical.Expression{
			Type:           TypeFromProto(expr.Type),
			ExpressionType: physical.ExpressionTypeVariable,
			Variable: &physical.Variable{
				Name:     typed.Variable.Name,
				IsLevel0: typed.Variable.IsLevel_0,
			},
		}
	case *Expression_Constant:
		return physical.Expression{
			Type:           TypeFromProto(expr.Type),
			ExpressionType: physical.ExpressionTypeConstant,
			Constant: &physical.Constant{
				Value: ValueFromProto(typed.Constant.Value),
			},
		}
	case *Expression_FunctionCall:
		arguments := make([]physical.Expression, len(typed.FunctionCall.Arguments))
		for i := range typed.FunctionCall.Arguments {
			arguments[i] = ExpressionFromProto(typed.FunctionCall.Arguments[i])
		}
		return physical.Expression{
			Type:           TypeFromProto(expr.Type),
			ExpressionType: physical.ExpressionTypeFunctionCall,
			FunctionCall: &physical.FunctionCall{
				Name:      typed.FunctionCall.Name,
				Arguments: arguments,
			},
		}
	case *Expression_And:
		arguments := make([]physical.Expression, len(typed.And.Arguments))
		for i := range typed.And.Arguments {
			arguments[i] = ExpressionFromProto(typed.And.Arguments[i])
		}
		return physical.Expression{
			Type:           TypeFromProto(expr.Type),
			ExpressionType: physical.ExpressionTypeAnd,
			And: &physical.And{
				Arguments: arguments,
			},
		}
	case *Expression_Or:
		arguments := make([]physical.Expression, len(typed.Or.Arguments))
		for i := range typed.Or.Arguments {
			arguments[i] = ExpressionFromProto(typed.Or.Arguments[i])
		}
		return physical.Expression{
			Type:           TypeFromProto(expr.Type),
			ExpressionType: physical.ExpressionTypeOr,
			Or: &physical.Or{
				Arguments: arguments,
			},
		}
	case *Expression_TypeAssertion:
		return physical.Expression{
			Type:           TypeFromProto(expr.Type),
			ExpressionType: physical.ExpressionTypeTypeAssertion,
			TypeAssertion: &physical.TypeAssertion{
				Expression: ExpressionFromProto(typed.TypeAssertion.Expression),
				TargetType: TypeFromProto(typed.TypeAssertion.TargetType),
			},
		}
	}

	panic("unexhaustive expression type match")
}

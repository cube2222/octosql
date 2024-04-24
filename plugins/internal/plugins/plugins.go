package plugins

import (
	"fmt"
	"log"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

const APILevel = 2

func (x *Record) ToNativeRecord() execution.Record {
	values := make([]octosql.Value, len(x.Values))
	for i := range x.Values {
		values[i] = x.Values[i].ToNativeValue()
	}
	return execution.Record{
		Values:     values,
		Retraction: x.Retraction,
		EventTime:  x.EventTime.AsTime(),
	}
}

func NativeRecordToProto(record execution.Record) *Record {
	values := make([]*Value, len(record.Values))
	for i := range record.Values {
		values[i] = NativeValueToProto(record.Values[i])
	}
	return &Record{
		Values:     values,
		Retraction: record.Retraction,
		EventTime:  timestamppb.New(record.EventTime),
	}
}

func (x *MetadataMessage) ToNativeMetadataMessage() execution.MetadataMessage {
	return execution.MetadataMessage{
		Type:      execution.MetadataMessageType(x.MessageType),
		Watermark: x.Watermark.AsTime(),
	}
}

func NativeMetadataMessageToProto(msg execution.MetadataMessage) *MetadataMessage {
	return &MetadataMessage{
		MessageType: int32(msg.Type),
		Watermark:   timestamppb.New(msg.Watermark),
	}
}

func NativeValueToProto(value octosql.Value) *Value {
	out := &Value{
		TypeId: int32(value.TypeID),
	}
	switch value.TypeID {
	case octosql.TypeIDNull:
	case octosql.TypeIDInt:
		out.Int = int64(value.Int)
	case octosql.TypeIDFloat:
		out.Float = value.Float
	case octosql.TypeIDBoolean:
		out.Boolean = value.Boolean
	case octosql.TypeIDString:
		out.Str = value.Str
	case octosql.TypeIDTime:
		out.Time = timestamppb.New(value.Time)
	case octosql.TypeIDDuration:
		out.Duration = durationpb.New(value.Duration)
	case octosql.TypeIDList:
		elements := make([]*Value, len(value.List))
		for i := range value.List {
			elements[i] = NativeValueToProto(value.List[i])
		}
		out.List = elements
	case octosql.TypeIDStruct:
		elements := make([]*Value, len(value.Struct))
		for i := range value.Struct {
			elements[i] = NativeValueToProto(value.Struct[i])
		}
		out.Struct = elements
	case octosql.TypeIDTuple:
		elements := make([]*Value, len(value.Tuple))
		for i := range value.Tuple {
			elements[i] = NativeValueToProto(value.Tuple[i])
		}
		out.Tuple = elements
	default:
		panic(fmt.Sprintf("invalid type to proto: %v %v", value.TypeID, value))
	}

	return out
}

func (x *Value) ToNativeValue() octosql.Value {
	out := octosql.Value{
		TypeID: octosql.TypeID(x.TypeId),
	}
	switch octosql.TypeID(x.TypeId) {
	case octosql.TypeIDNull:
	case octosql.TypeIDInt:
		out.Int = x.Int
	case octosql.TypeIDFloat:
		out.Float = x.Float
	case octosql.TypeIDBoolean:
		out.Boolean = x.Boolean
	case octosql.TypeIDString:
		out.Str = x.Str
	case octosql.TypeIDTime:
		out.Time = x.Time.AsTime()
	case octosql.TypeIDDuration:
		out.Duration = x.Duration.AsDuration()
	case octosql.TypeIDList:
		elements := make([]octosql.Value, len(x.List))
		for i := range x.List {
			elements[i] = x.List[i].ToNativeValue()
		}
		out.List = elements
	case octosql.TypeIDStruct:
		elements := make([]octosql.Value, len(x.Struct))
		for i := range x.Struct {
			elements[i] = x.Struct[i].ToNativeValue()
		}
		out.Struct = elements
	case octosql.TypeIDTuple:
		elements := make([]octosql.Value, len(x.Tuple))
		for i := range x.Tuple {
			elements[i] = x.Tuple[i].ToNativeValue()
		}
		out.Tuple = elements
	default:
		panic(fmt.Sprintf("invalid type to proto: %v %v", x.TypeId, x))
	}
	return out
}

func NativeSchemaToProto(schema physical.Schema) *Schema {
	fields := make([]*SchemaField, len(schema.Fields))
	for i := range schema.Fields {
		fields[i] = &SchemaField{
			Name: schema.Fields[i].Name,
			Type: NativeTypeToProto(schema.Fields[i].Type),
		}
	}
	return &Schema{
		Fields:        fields,
		TimeField:     int32(schema.TimeField),
		NoRetractions: schema.NoRetractions,
	}
}

func (x *Schema) ToNativeSchema() physical.Schema {
	fields := make([]physical.SchemaField, len(x.Fields))
	for i := range x.Fields {
		fields[i] = physical.SchemaField{
			Name: x.Fields[i].Name,
			Type: x.Fields[i].Type.ToNativeType(),
		}
	}
	return physical.Schema{
		Fields:        fields,
		TimeField:     int(x.TimeField),
		NoRetractions: x.NoRetractions,
	}
}

func NativeTypeToProto(t octosql.Type) *Type {
	out := &Type{
		TypeId: int32(t.TypeID),
	}
	switch t.TypeID {
	case octosql.TypeIDNull, octosql.TypeIDInt, octosql.TypeIDFloat, octosql.TypeIDBoolean, octosql.TypeIDString, octosql.TypeIDTime, octosql.TypeIDDuration, octosql.TypeIDAny:
	case octosql.TypeIDList:
		if t.List.Element != nil {
			out.List = NativeTypeToProto(*t.List.Element)
		}
	case octosql.TypeIDStruct:
		elements := make([]*StructField, len(t.Struct.Fields))
		for i := range t.Struct.Fields {
			elements[i] = &StructField{
				Name: t.Struct.Fields[i].Name,
				Type: NativeTypeToProto(t.Struct.Fields[i].Type),
			}
		}
		out.Struct = elements
	case octosql.TypeIDTuple:
		elements := make([]*Type, len(t.Tuple.Elements))
		for i := range t.Tuple.Elements {
			elements[i] = NativeTypeToProto(t.Tuple.Elements[i])
		}
		out.Tuple = elements
	case octosql.TypeIDUnion:
		elements := make([]*Type, len(t.Union.Alternatives))
		for i := range t.Union.Alternatives {
			elements[i] = NativeTypeToProto(t.Union.Alternatives[i])
		}
		out.Union = elements
	default:
		panic(fmt.Sprintf("invalid type to proto: %v %v", t.TypeID, t))
	}
	return out
}

func (x *Type) ToNativeType() octosql.Type {
	out := octosql.Type{
		TypeID: octosql.TypeID(x.TypeId),
	}
	switch octosql.TypeID(x.TypeId) {
	case octosql.TypeIDNull, octosql.TypeIDInt, octosql.TypeIDFloat, octosql.TypeIDBoolean, octosql.TypeIDString, octosql.TypeIDTime, octosql.TypeIDDuration, octosql.TypeIDAny:
	case octosql.TypeIDList:
		if x.List != nil {
			t := x.List.ToNativeType()
			out.List.Element = &t
		}
	case octosql.TypeIDStruct:
		elements := make([]octosql.StructField, len(x.Struct))
		for i := range x.Struct {
			elements[i] = octosql.StructField{
				Name: x.Struct[i].Name,
				Type: x.Struct[i].Type.ToNativeType(),
			}
		}
		out.Struct.Fields = elements
	case octosql.TypeIDTuple:
		elements := make([]octosql.Type, len(x.Tuple))
		for i := range x.Tuple {
			elements[i] = x.Tuple[i].ToNativeType()
		}
		out.Tuple.Elements = elements
	case octosql.TypeIDUnion:
		elements := make([]octosql.Type, len(x.Union))
		for i := range x.Union {
			elements[i] = x.Union[i].ToNativeType()
		}
		out.Union.Alternatives = elements
	default:
		panic(fmt.Sprintf("invalid proto to type: %v %v", x.TypeId, x))
	}
	return out
}

func NativePhysicalVariableContextToProto(c *physical.VariableContext) *PhysicalVariableContext {
	frames := make([]*PhysicalVariableContextFrame, 0)
	for c != nil {
		fields := make([]*SchemaField, len(c.Fields))
		for i := range c.Fields {
			fields[i] = &SchemaField{
				Name: c.Fields[i].Name,
				Type: NativeTypeToProto(c.Fields[i].Type),
			}
		}
		frames = append(frames, &PhysicalVariableContextFrame{
			Fields: fields,
		})
		c = c.Parent
	}
	return &PhysicalVariableContext{
		Frames: frames,
	}
}

func (x *PhysicalVariableContext) ToNativePhysicalVariableContext() *physical.VariableContext {
	var out *physical.VariableContext
	for i := len(x.Frames) - 1; i >= 0; i-- {
		fields := make([]physical.SchemaField, len(x.Frames[i].Fields))
		for j := range x.Frames[i].Fields {
			fields[j] = physical.SchemaField{
				Name: x.Frames[i].Fields[j].Name,
				Type: x.Frames[i].Fields[j].Type.ToNativeType(),
			}
		}
		out = &physical.VariableContext{
			Fields: fields,
			Parent: out,
		}
	}
	return out
}

func NativeExecutionVariableContextToProto(c *execution.VariableContext) *ExecutionVariableContext {
	frames := make([]*ExecutionVariableContextFrame, 0)
	for c != nil {
		values := make([]*Value, len(c.Values))
		for i := range c.Values {
			values[i] = NativeValueToProto(c.Values[i])
		}
		frames = append(frames, &ExecutionVariableContextFrame{
			Values: values,
		})
		c = c.Parent
	}
	return &ExecutionVariableContext{
		Frames: frames,
	}
}

func (x *ExecutionVariableContext) ToNativeExecutionVariableContext() *execution.VariableContext {
	var out *execution.VariableContext
	for i := len(x.Frames) - 1; i >= 0; i-- {
		values := make([]octosql.Value, len(x.Frames[i].Values))
		for j := range x.Frames[i].Values {
			values[j] = x.Frames[i].Values[j].ToNativeValue()
		}
		out = &execution.VariableContext{
			Values: values,
			Parent: out,
		}
	}
	return out
}

func RepopulatePhysicalExpressionFunctions(expr physical.Expression) (physical.Expression, bool) {
	funcMap := functions.FunctionMap()

	outOk := true
	out := (&physical.Transformers{
		ExpressionTransformer: func(expr physical.Expression) physical.Expression {
			if expr.ExpressionType != physical.ExpressionTypeFunctionCall {
				return expr
			}
			receivedDescriptor := expr.FunctionCall.FunctionDescriptor

			details, ok := funcMap[expr.FunctionCall.Name]
			if !ok {
				log.Printf("Unknown function, rejecting predicate: %s", expr.FunctionCall.Name)
				outOk = false
				return expr
			}

		descriptorLoop:
			for _, descriptor := range details.Descriptors {
				if len(descriptor.ArgumentTypes) != len(receivedDescriptor.ArgumentTypes) {
					continue descriptorLoop
				}
				if descriptor.Strict != receivedDescriptor.Strict {
					continue descriptorLoop
				}
				if !descriptor.OutputType.Equals(receivedDescriptor.OutputType) {
					continue descriptorLoop
				}
				for j := range descriptor.ArgumentTypes {
					if !descriptor.ArgumentTypes[j].Equals(receivedDescriptor.ArgumentTypes[j]) {
						continue descriptorLoop
					}
				}
				expr.FunctionCall.FunctionDescriptor.TypeFn = descriptor.TypeFn
				expr.FunctionCall.FunctionDescriptor.Function = descriptor.Function
				return expr
			}

			log.Printf("Unknown function signature, rejecting predicate: %s", expr.FunctionCall.Name)
			ok = false
			return expr
		},
	}).TransformExpr(expr)
	return out, outOk
}

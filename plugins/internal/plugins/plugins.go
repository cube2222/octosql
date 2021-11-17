package plugins

import (
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

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
		out.Int = int(x.Int)
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

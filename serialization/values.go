package serialization

import (
	"fmt"
	"log"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/serialization/internal"
)

func Serialize(value octosql.Value) []byte {
	protoValue := octoToProto(value)
	data, err := proto.Marshal(protoValue)
	if err != nil {
		// octosql.Value is normalized, it should always be serializable
		panic(fmt.Sprintf("error marshaling proto octosql.Value: %v error: %v", value, err))
	}
	return data
}

func Deserialize(data []byte) (octosql.Value, error) {
	var value internal.Value
	err := proto.Unmarshal(data, &value)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't deserialize proto to octosql.Value")
	}

	return protoToOcto(&value), nil
}

func octoToProto(value octosql.Value) *internal.Value {
	switch value := value.(type) {
	case octosql.Null:
		return &internal.Value{
			Value: &internal.Value_Null{},
		}

	case octosql.Phantom:
		return &internal.Value{
			Value: &internal.Value_Phantom{},
		}

	case octosql.Int:
		return &internal.Value{
			Value: &internal.Value_Int{
				Int: int64(value.AsInt()),
			},
		}

	case octosql.Float:
		return &internal.Value{
			Value: &internal.Value_Float{
				Float: value.AsFloat(),
			},
		}

	case octosql.Bool:
		return &internal.Value{
			Value: &internal.Value_Bool{
				Bool: value.AsBool(),
			},
		}

	case octosql.String:
		return &internal.Value{
			Value: &internal.Value_String_{
				String_: value.AsString(),
			},
		}

	case octosql.Time:
		t, err := ptypes.TimestampProto(value.AsTime())
		if err != nil {
			panic(fmt.Sprintf("invalid time to proto timestamp: %v", err))
		}
		return &internal.Value{
			Value: &internal.Value_Time{
				Time: t,
			},
		}

	case octosql.Duration:
		return &internal.Value{
			Value: &internal.Value_Duration{
				Duration: ptypes.DurationProto(value.AsDuration()),
			},
		}

	case octosql.Tuple:
		tuple := &internal.Tuple{
			Fields: make([]*internal.Value, len(value.AsSlice())),
		}
		for i, v := range value.AsSlice() {
			tuple.Fields[i] = octoToProto(v)
		}

		return &internal.Value{
			Value: &internal.Value_Tuple{
				Tuple: tuple,
			},
		}

	case octosql.Object:
		object := &internal.Object{
			Fields: make(map[string]*internal.Value),
		}
		for k, v := range value.AsMap() {
			object.Fields[k] = octoToProto(v)
		}

		return &internal.Value{
			Value: &internal.Value_Object{
				Object: object,
			},
		}
	}
	log.Fatalf("unhandled type of octosql.Value: %v", reflect.TypeOf(value).String())
	panic("unreachable")
}

func protoToOcto(value *internal.Value) octosql.Value {
	switch value := value.Value.(type) {
	case *internal.Value_Null:
		return octosql.MakeNull()

	case *internal.Value_Phantom:
		return octosql.MakePhantom()

	case *internal.Value_Int:
		return octosql.MakeInt(int(value.Int))

	case *internal.Value_Float:
		return octosql.MakeFloat(value.Float)

	case *internal.Value_Bool:
		return octosql.MakeBool(value.Bool)

	case *internal.Value_String_:
		return octosql.MakeString(value.String_)

	case *internal.Value_Time:
		t, err := ptypes.Timestamp(value.Time)
		if err != nil {
			panic(fmt.Sprintf("invalid proto timestamp to time: %v", err))
		}
		return octosql.MakeTime(t)

	case *internal.Value_Duration:
		dur, err := ptypes.Duration(value.Duration)
		if err != nil {
			panic(fmt.Sprintf("invalid proto duration to time: %v", err))
		}
		return octosql.MakeDuration(dur)

	case *internal.Value_Tuple:
		tuple := make([]octosql.Value, len(value.Tuple.Fields))
		for i, v := range value.Tuple.Fields {
			tuple[i] = protoToOcto(v)
		}

		return octosql.MakeTuple(tuple)

	case *internal.Value_Object:
		object := make(map[string]octosql.Value)
		for k, v := range value.Object.Fields {
			object[k] = protoToOcto(v)
		}

		return octosql.MakeObject(object)
	}
	log.Fatalf("unhandled type of octosql.Value: %v", reflect.TypeOf(value).String())
	panic("unreachable")
}

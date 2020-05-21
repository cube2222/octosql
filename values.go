package octosql

import (
	"encoding/base32"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/docs"
)

// go-sumtype:decl Value
// type Value interface {
//	docs.Documented

func MakeNull() Value {
	return Value{Value: &Value_Nulls{Nulls: &Nulls{}}}
}
func ZeroNull() Value {
	return Value{Value: &Value_Nulls{Nulls: &Nulls{}}}
}

type Phantom struct{}

func MakePhantom() Value {
	return Value{Value: &Value_Phantom{Phantom: &Phantoms{}}}
}
func ZeroPhantom() Value {
	return Value{Value: &Value_Phantom{Phantom: &Phantoms{}}}
}

type Int int

func MakeInt(v []int) Value {
	ints := make([]int64, len(v))
	for i := range v {
		ints[i] = int64(v[i])
	}
	return Value{Value: &Value_Int{Int: &Ints{Ints: ints}}}
}

func ZeroInt() Value {
	return Value{Value: &Value_Int{Int: &Ints{}}}
}

type Float float64

func MakeFloat(v []float64) Value {
	return Value{Value: &Value_Float{Float: &Floats{Floats: v}}}
}
func ZeroFloat() Value {
	return Value{Value: &Value_Float{Float: &Floats{}}}
}

func MakeBool(v []bool) Value {
	return Value{Value: &Value_Bool{Bool: &Bools{Bools: v}}}
}
func ZeroBool() Value {
	return Value{Value: &Value_Bool{Bool: &Bools{}}}
}

func MakeString(v []string) Value {
	return Value{Value: &Value_String_{String_: &Strings{Strings: v}}}
}
func ZeroString() Value {
	return Value{Value: &Value_String_{String_: &Strings{}}}
}

func MakeTime(v []time.Time) Value {
	times := make([]*timestamp.Timestamp, len(v))
	for i := range v {
		t, err := ptypes.TimestampProto(v[i])
		if err != nil {
			panic(err)
		}
		times[i] = t
	}
	return Value{Value: &Value_Time{Time: &Times{Times: times}}}
}
func ZeroTime() Value {
	return Value{Value: &Value_Time{Time: &Times{}}}
}

func MakeDuration(v []time.Duration) Value {
	durations := make([]*duration.Duration, len(v))
	for i := range v {
		durations[i] = ptypes.DurationProto(v[i])
	}
	return Value{Value: &Value_Duration{Duration: &Durations{Durations: durations}}}
}
func ZeroDuration() Value {
	return Value{Value: &Value_Duration{Duration: &Durations{}}}
}

func MakeTuple(v []Value) Value {
	tuple := &Tuple{
		Fields: make([]*Value, len(v)),
	}
	for i, v := range v {
		vInternal := v
		tuple.Fields[i] = &vInternal
	}
	return Value{Value: &Value_Tuple{Tuple: tuple}}
}
func ZeroTuple() Value {
	return Value{Value: &Value_Tuple{Tuple: &Tuple{
		Fields: nil,
	}}}
}

func MakeObject(v map[string]Value) Value {
	object := &Object{
		Fields: make(map[string]*Value),
	}
	for k, v := range v {
		vInternal := v
		object.Fields[k] = &vInternal
	}

	return Value{Value: &Value_Object{Object: object}}
}

func ZeroObject() Value {
	return Value{Value: &Value_Object{Object: &Object{
		Fields: nil,
	}}}
}

// NormalizeType brings various primitive types into the type we want them to be.
// All types coming out of data sources have to be already normalized this way.
func NormalizeType(value interface{}) Value {
	switch value := value.(type) {
	case nil:
		return MakeNull()
	case []bool:
		return MakeBool(value)
	case []int:
		return MakeInt(value)
	case []int8:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []int16:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []int32:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []int64:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []uint8:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []uint16:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []uint32:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []uint64:
		ints := make([]int, len(value))
		for i := range value {
			ints[i] = int(value[i])
		}
		return MakeInt(ints)
	case []float32:
		floats := make([]float64, len(value))
		for i := range value {
			floats[i] = float64(value[i])
		}
		return MakeFloat(floats)
	case []float64:
		return MakeFloat(value)
	case [][]byte:
		encoded := make([]string, len(value))
		for i := range value {
			encoded[i] = base32.StdEncoding.EncodeToString(value[i])
		}
		return MakeString(encoded)
	case []string:
		return MakeString(value)
	case [][]interface{}:
		out := make([]Value, len(value))
		for i := range value {
			out[i] = NormalizeType(value[i])
		}
		return MakeTuple(out)
	case []map[string]interface{}:
		if len(value) == 0 {
			return MakeObject(nil)
		}
		out := make(map[string]Value)
		for k := range value[0] {
			t := reflect.TypeOf(value[0][k])
			toNormalize := reflect.MakeSlice(t, len(value), len(value))
			for i := range value {
				toNormalize.Index(i).Set(reflect.ValueOf(value[i][k]))
			}
			out[k] = NormalizeType(toNormalize.Interface())
		}
		return MakeObject(out)
	case []time.Time:
		return MakeTime(value)
	case []time.Duration:
		return MakeDuration(value)
	case []struct{}:
		return MakePhantom()
	case Value:
		return value
	}
	log.Fatalf("invalid type to normalize: %s", reflect.TypeOf(value).String())
	panic("unreachable")
}

// octosql.AreEqual checks the equality of the given values, returning false if the types don't match.
func AreEqual(left, right Value) bool {
	return proto.Equal(&left, &right)
}

type Comparison int

const (
	LessThan    Comparison = -1
	Equal       Comparison = 0
	GreaterThan Comparison = 1
)

func Compare(x, y Value) (Comparison, error) {
	switch x.GetType() {
	case TypeInt:
		if y.GetType() != TypeInt {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsInt()
		y := y.AsInt()

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case TypeFloat:
		if y.GetType() != TypeFloat {
			return 0, errors.Errorf("type mismatch between values")
		}
		x := x.AsFloat()
		y := y.AsFloat()

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case TypeString:
		if y.GetType() != TypeString {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsString()
		y := y.AsString()

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case TypeTime:
		if y.GetType() != TypeTime {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsTime()
		y := y.AsTime()

		if x == y {
			return 0, nil
		} else if x.Before(y) {
			return -1, nil
		}

		return 1, nil
	case TypeBool:
		if y.GetType() != TypeBool {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsBool()
		y := y.AsBool()

		if x == y {
			return 0, nil
		} else if !x && y {
			return -1, nil
		}

		return 1, nil

	case TypeNull, TypePhantom, TypeDuration, TypeTuple, TypeObject:
		return 0, errors.Errorf("unsupported type in sorting")
	}

	panic("unreachable")
}

func ZeroValue() Value {
	return Value{}
}

func (v Value) AsInt() []int {
	oldInts := v.GetInt().Ints
	ints := make([]int, len(oldInts))
	for i := range oldInts {
		ints[i] = int(oldInts[i])
	}
	return ints
}

func (v Value) AsFloat() []float64 {
	return v.GetFloat().Floats
}

func (v Value) AsBool() []bool {
	return v.GetBool().Bools
}

func (v Value) AsString() []string {
	return v.GetString_().Strings
}

func (v Value) AsTime() []time.Time {
	oldTimes := v.GetTime().Times
	times := make([]time.Time, len(oldTimes))
	for i := range oldTimes {
		t, err := ptypes.Timestamp(oldTimes[i])
		if err != nil {
			panic(err)
		}
		times[i] = t
	}
	return times
}

func (v Value) AsDuration() []time.Duration {
	oldDurations := v.GetDuration().Durations
	durations := make([]time.Duration, len(oldDurations))
	for i := range oldDurations {
		d, err := ptypes.Duration(oldDurations[i])
		if err != nil {
			panic(err)
		}
		durations[i] = d
	}
	return durations
}

func (v Value) AsSlice() []Value {
	t := v.GetTuple()
	out := make([]Value, len(t.Fields))
	for i := range out {
		out[i] = *t.Fields[i]
	}
	return out
}

func (v Value) AsMap() map[string]Value {
	obj := v.GetObject()
	out := make(map[string]Value)
	for k, v := range obj.Fields {
		out[k] = *v
	}
	return out
}

type Type int

const (
	TypeZero Type = iota
	TypeNull
	TypePhantom
	TypeInt
	TypeFloat
	TypeBool
	TypeString
	TypeTime
	TypeDuration
	TypeTuple
	TypeObject
)

func (t Type) String() string {
	switch t {
	case TypeZero:
		return "Zero"
	case TypeNull:
		return "Null"
	case TypePhantom:
		return "Phantom"
	case TypeInt:
		return "Int"
	case TypeFloat:
		return "Float"
	case TypeBool:
		return "Bool"
	case TypeString:
		return "String"
	case TypeTime:
		return "Time"
	case TypeDuration:
		return "Duration"
	case TypeTuple:
		return "Tuple"
	case TypeObject:
		return "Object"
	default:
		panic("invalid type")
	}
}

// Można na tych Value pod spodem zdefiniowac GetType i użyć wirtualnych metod, a nie type switch
func (v Value) GetType() Type {
	switch v.Value.(type) {
	case *Value_Null:
		return TypeNull
	case *Value_Phantom:
		return TypePhantom
	case *Value_Int:
		return TypeInt
	case *Value_Float:
		return TypeFloat
	case *Value_Bool:
		return TypeBool
	case *Value_String_:
		return TypeString
	case *Value_Time:
		return TypeTime
	case *Value_Duration:
		return TypeDuration
	case *Value_Tuple:
		return TypeTuple
	case *Value_Object:
		return TypeObject
	default:
		return TypeZero
	}
}

func (v Value) Document() docs.Documentation {
	switch v.GetType() {
	case TypeZero:
		return docs.Text("Zero Value")
	case TypeNull:
		return docs.Text("Null")
	case TypePhantom:
		return docs.Text("Phantom")
	case TypeInt:
		return docs.Text("Int")
	case TypeFloat:
		return docs.Text("Float")
	case TypeBool:
		return docs.Text("Bool")
	case TypeString:
		return docs.Text("String")
	case TypeTime:
		return docs.Text("Time")
	case TypeDuration:
		return docs.Text("Duration")
	case TypeTuple:
		return docs.Text("Tuple")
	case TypeObject:
		return docs.Text("Object")
	default:
		panic("invalid type")
	}
}

func (v Value) Show() string {
	switch v.GetType() {
	case TypeZero:
		return "<zeroValue>"
	case TypeNull:
		return "<null>"
	case TypePhantom:
		return "<phantom>"
	case TypeInt:
		return fmt.Sprint(v.AsInt())
	case TypeFloat:
		return fmt.Sprint(v.AsFloat())
	case TypeBool:
		return fmt.Sprint(v.AsBool())
	case TypeString:
		return fmt.Sprintf("'%s'", v.AsString())
	case TypeTime:
		// return v.AsTime().Format(time.RFC3339Nano)
		return fmt.Sprintf("%+v", v.AsTime())
	case TypeDuration:
		// return v.AsDuration().String()
		return fmt.Sprintf("%+v", v.AsDuration())
	case TypeTuple:
		valueStrings := make([]string, len(v.AsSlice()))
		for i, value := range v.AsSlice() {
			valueStrings[i] = value.Show()
		}
		return fmt.Sprintf("(%s)", strings.Join(valueStrings, ", "))
	case TypeObject:
		pairStrings := make([]string, 0, len(v.AsMap()))
		for k, v := range v.AsMap() {
			pairStrings = append(pairStrings, fmt.Sprintf("%s: %s", k, v.Show()))
		}
		return fmt.Sprintf("{%s}", strings.Join(pairStrings, ", "))
	default:
		panic("invalid type")
	}
}

func (v Value) ToRawValue() interface{} {
	switch v.GetType() {
	case TypeZero:
		return nil
	case TypeNull:
		return nil
	case TypePhantom:
		return struct{}{}
	case TypeInt:
		return v.AsInt()
	case TypeFloat:
		return v.AsFloat()
	case TypeBool:
		return v.AsBool()
	case TypeString:
		return v.AsString()
	case TypeTime:
		return v.AsTime()
	case TypeDuration:
		return v.AsDuration()
	case TypeTuple:
		out := make([]interface{}, len(v.AsSlice()))
		for i, v := range v.AsSlice() {
			out[i] = v.ToRawValue()
		}
		return out
	case TypeObject:
		out := make(map[string]interface{}, len(v.AsMap()))
		for k, v := range v.AsMap() {
			out[k] = v.ToRawValue()
		}
		return out
	default:
		return nil
	}
}

func GetValuesFromPointers(values []*Value) []Value {
	result := make([]Value, len(values))
	for i, v := range values {
		result[i] = *v
	}

	return result
}

func GetPointersFromValues(values []Value) []*Value {
	result := make([]*Value, len(values))
	for i, _ := range values {
		result[i] = &values[i]
	}

	return result
}

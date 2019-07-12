package octosql

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
)

type Value interface {
	octoValue()
}

type Phantom struct{}

func (Phantom) octoValue()         {}
func (v Phantom) Struct() struct{} { return struct{}(v) }
func MakePhantom() Phantom {
	return Phantom(struct{}{})
}

type Int int

func (Int) octoValue() {}
func (v Int) Int() int { return int(v) }
func MakeInt(v int) Int {
	return Int(v)
}

type Float float64

func (Float) octoValue()       {}
func (v Float) Float() float64 { return float64(v) }
func MakeFloat(v float64) Float {
	return Float(v)
}

type Bool bool

func (Bool) octoValue()   {}
func (v Bool) Bool() bool { return bool(v) }
func MakeBool(v bool) Bool {
	return Bool(v)
}

type String string

func (String) octoValue()       {}
func (v String) String() string { return string(v) }
func MakeString(v string) String {
	return String(v)
}

type Time time.Time

func (Time) octoValue()        {}
func (v Time) Time() time.Time { return time.Time(v) }
func MakeTime(v time.Time) Time {
	return Time(v)
}

type Tuple []Value

func (Tuple) octoValue()       {}
func (v Tuple) Slice() []Value { return []Value(v) }
func (v Tuple) String() string {
	valueStrings := make([]string, len(v.Slice()))
	for i, value := range v.Slice() {
		valueStrings[i] = fmt.Sprint(value)
	}
	return fmt.Sprintf("(%s)", strings.Join(valueStrings, ", "))
}
func MakeTuple(v []Value) Tuple {
	return Tuple(v)
}

type Object map[string]Value

func (Object) octoValue()                 {}
func (v Object) Object() map[string]Value { return map[string]Value(v) }
func MakeObject(v map[string]Value) Object {
	return Object(v)
}

// NormalizeType brings various primitive types into the type we want them to be.
// All types coming out of data sources have to be already normalized this way.
func NormalizeType(value interface{}) Value {
	switch value := value.(type) {
	case bool:
		return MakeBool(value)
	case int:
		return MakeInt(value)
	case int8:
		return MakeInt(int(value))
	case int32:
		return MakeInt(int(value))
	case int64:
		return MakeInt(int(value))
	case uint8:
		return MakeInt(int(value))
	case uint32:
		return MakeInt(int(value))
	case uint64:
		return MakeInt(int(value))
	case float32:
		return MakeFloat(float64(value))
	case float64:
		return MakeFloat(value)
	case []byte:
		return MakeString(string(value))
	case string:
		return MakeString(value)
	case []interface{}:
		out := make(Tuple, len(value))
		for i := range value {
			out[i] = NormalizeType(value[i])
		}
		return out
	case map[string]interface{}:
		out := make(Object)
		for k, v := range value {
			out[k] = NormalizeType(v)
		}
		return out
	case *interface{}:
		if value != nil {
			return NormalizeType(*value)
		}
		return nil
	case time.Time:
		return MakeTime(value)
	case struct{}:
		return MakePhantom()
	case Value:
		return value
	}
	log.Fatalf("invalid type to normalize: %s", reflect.TypeOf(value).String())
	panic("unreachable")
}

// octosql.AreEqual checks the equality of the given values, returning false if the types don't match.
func AreEqual(left, right Value) bool {
	if left == nil && right == nil {
		return true
	}
	switch left := left.(type) {
	case Int:
		right, ok := right.(Int)
		if !ok {
			return false
		}
		return left == right

	case Float:
		right, ok := right.(Float)
		if !ok {
			return false
		}
		return left == right

	case Bool:
		right, ok := right.(Bool)
		if !ok {
			return false
		}
		return left == right

	case String:
		right, ok := right.(String)
		if !ok {
			return false
		}
		return left == right

	case Time:
		right, ok := right.(Time)
		if !ok {
			return false
		}
		return left.Time().Equal(right.Time())

	case Tuple:
		right, ok := right.(Tuple)
		if !ok {
			return false
		}
		if len(left) != len(right) {
			return false
		}
		for i := range left {
			if !AreEqual(left[i], right[i]) {
				return false
			}
		}
		return true

	case Object:
		right, ok := right.(Object)
		if !ok {
			return false
		}
		if len(left) != len(right) {
			return false
		}
		for k := range left {
			if !AreEqual(left[k], right[k]) {
				return false
			}
		}
		return true

	}
	log.Fatalf("unhandled type of octosql.Value: %v", reflect.TypeOf(left).String())
	panic("unreachable")
}

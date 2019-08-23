package octosql

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/cube2222/octosql/docs"
)

//go-sumtype:decl Value
type Value interface {
	docs.Documented
	octoValue()
	fmt.Stringer
}

type Null struct{}

func (Null) octoValue()            {}
func (v Null) AsNull() interface{} { return nil }
func (v Null) String() string {
	return "<null>"
}
func (v Null) Document() docs.Documentation {
	return docs.Text("Null")
}
func MakeNull() Null {
	return Null(struct{}{})
}
func ZeroNull() Null {
	return struct{}{}
}

type Phantom struct{}

func (Phantom) octoValue()           {}
func (v Phantom) AsStruct() struct{} { return struct{}(v) }
func (v Phantom) String() string {
	return "<phantom>"
}
func (v Phantom) Document() docs.Documentation {
	return docs.Text("Phantom")
}
func MakePhantom() Phantom {
	return Phantom(struct{}{})
}
func ZeroPhantom() Phantom {
	return struct{}{}
}

type Int int

func (Int) octoValue()   {}
func (v Int) AsInt() int { return int(v) }
func (v Int) String() string {
	return fmt.Sprint(v.AsInt())
}
func (v Int) Document() docs.Documentation {
	return docs.Text("Int")
}
func MakeInt(v int) Int {
	return Int(v)
}
func ZeroInt() Int {
	return 0
}

type Float float64

func (Float) octoValue()         {}
func (v Float) AsFloat() float64 { return float64(v) }
func (v Float) String() string {
	return fmt.Sprint(v.AsFloat())
}
func (v Float) Document() docs.Documentation {
	return docs.Text("Float")
}
func MakeFloat(v float64) Float {
	return Float(v)
}
func ZeroFloat() Float {
	return 0
}

type Bool bool

func (Bool) octoValue()     {}
func (v Bool) AsBool() bool { return bool(v) }
func (v Bool) String() string {
	return fmt.Sprint(v.AsBool())
}
func (v Bool) Document() docs.Documentation {
	return docs.Text("Bool")
}
func MakeBool(v bool) Bool {
	return Bool(v)
}
func ZeroBool() Bool {
	return false
}

type String string

func (String) octoValue()         {}
func (v String) AsString() string { return string(v) }
func (v String) String() string {
	return fmt.Sprintf("'%s'", v.AsString())
}
func (v String) Document() docs.Documentation {
	return docs.Text("String")
}
func MakeString(v string) String {
	return String(v)
}
func ZeroString() String {
	return ""
}

type Time time.Time

func (Time) octoValue()          {}
func (v Time) AsTime() time.Time { return time.Time(v) }
func (v Time) String() string {
	return v.AsTime().Format(time.RFC3339Nano)
}
func (v Time) Document() docs.Documentation {
	return docs.Text("Time")
}
func MakeTime(v time.Time) Time {
	return Time(v)
}
func ZeroTime() Time {
	return Time(time.Time{})
}

type Duration time.Duration

func (Duration) octoValue()                  {}
func (v Duration) AsDuration() time.Duration { return time.Duration(v) }
func (v Duration) String() string {
	return v.AsDuration().String()
}
func (v Duration) Document() docs.Documentation {
	return docs.Text("Duration")
}
func MakeDuration(v time.Duration) Duration {
	return Duration(v)
}
func ZeroDuration() Duration {
	return Duration(time.Duration(0))
}

type Tuple []Value

func (Tuple) octoValue()         {}
func (v Tuple) AsSlice() []Value { return []Value(v) }
func (v Tuple) String() string {
	valueStrings := make([]string, len(v.AsSlice()))
	for i, value := range v.AsSlice() {
		valueStrings[i] = fmt.Sprint(value)
	}
	return fmt.Sprintf("(%s)", strings.Join(valueStrings, ", "))
}
func (v Tuple) Document() docs.Documentation {
	return docs.Text("Tuple")
}
func MakeTuple(v []Value) Tuple {
	return Tuple(v)
}
func ZeroTuple() Tuple {
	return nil
}

type Object map[string]Value

func (Object) octoValue()                {}
func (v Object) AsMap() map[string]Value { return map[string]Value(v) }
func (v Object) String() string {
	text, err := json.Marshal(v.AsMap())
	if err != nil {
		return fmt.Sprint(v.AsMap())
	}
	return string(text)
}
func (v Object) Document() docs.Documentation {
	return docs.Text("Object")
}
func MakeObject(v map[string]Value) Object {
	return Object(v)
}
func ZeroObject() Object {
	return nil
}

// NormalizeType brings various primitive types into the type we want them to be.
// All types coming out of data sources have to be already normalized this way.
func NormalizeType(value interface{}) Value {
	switch value := value.(type) {
	case nil:
		return MakeNull()
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
	case time.Duration:
		return MakeDuration(value)
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

	if (left == nil && right != nil) ||
		(left != nil && right == nil) {
		return false
	}

	switch left := left.(type) {
	case Null:
		_, ok := right.(Null)
		if !ok {
			return false
		}
		return true

	case Phantom:
		_, ok := right.(Phantom)
		if !ok {
			return false
		}
		return true

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

		return left.AsTime().Equal(right.AsTime())

	case Duration:
		right, ok := right.(Duration)
		if !ok {
			return false
		}
		return left == right

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

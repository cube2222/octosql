package octosql

import "time"

type Value interface {
	OctoValue()
}

type Phantom struct{}

func (Phantom) OctoValue()         {}
func (v Phantom) Struct() struct{} { return struct{}(v) }
func MakePhantom() Phantom {
	return Phantom(struct{}{})
}

type Int int

func (Int) OctoValue() {}
func (v Int) Int() int { return int(v) }
func MakeInt(v int) Int {
	return Int(v)
}

type Float float64

func (Float) OctoValue()       {}
func (v Float) Float() float64 { return float64(v) }
func MakeFloat(v float64) Float {
	return Float(v)
}

type Bool bool

func (Bool) OctoValue()   {}
func (v Bool) Bool() bool { return bool(v) }
func MakeBool(v bool) Bool {
	return Bool(v)
}

type String string

func (String) OctoValue()       {}
func (v String) String() string { return string(v) }
func MakeString(v string) String {
	return String(v)
}

type Time time.Time

func (Time) OctoValue()        {}
func (v Time) Time() time.Time { return time.Time(v) }
func MakeTime(v time.Time) Time {
	return Time(v)
}

type Tuple []Value

func (Tuple) OctoValue()       {}
func (v Tuple) Slice() []Value { return []Value(v) }
func MakeTuple(v []Value) Tuple {
	return Tuple(v)
}

type Object map[string]Value

func (Object) OctoValue()                 {}
func (v Object) Object() map[string]Value { return map[string]Value(v) }
func MakeObject(v map[string]Value) Object {
	return Object(v)
}

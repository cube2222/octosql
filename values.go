package octosql

import "time"

var ZeroValue = Value{}

type Value struct {
	Type        Type
	Int         int
	Float       float64
	Boolean     bool
	Str         string
	Time        time.Time
	Duration    time.Duration
	List        []Value
	FieldValues []Value
}

func NewInt(value int) Value {
	return Value{
		Type: Type{TypeID: TypeIDInt},
		Int:  value,
	}
}
func NewFloat(value float64) Value {
	return Value{
		Type:  Type{TypeID: TypeIDFloat},
		Float: value,
	}
}
func NewBoolean(value bool) Value {
	return Value{
		Type:    Type{TypeID: TypeIDBoolean},
		Boolean: value,
	}
}
func NewString(value string) Value {
	return Value{
		Type: Type{TypeID: TypeIDString},
		Str:  value,
	}
}
func NewTime(value time.Time) Value {
	return Value{
		Type: Type{TypeID: TypeIDTime},
		Time: value,
	}
}
func NewDuration(value time.Duration) Value {
	return Value{
		Type:     Type{TypeID: TypeIDDuration},
		Duration: value,
	}
}
func NewList(value []Value) Value {
	return Value{
		Type: Type{TypeID: TypeIDList},
		List: value,
	}
}
func NewFieldValues(value []Value) Value {
	return Value{
		Type:        Type{TypeID: TypeIDString},
		FieldValues: value,
	}
}

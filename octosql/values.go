package octosql

import (
	"fmt"
	"strings"
	"time"
)

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
	Tuple       []Value
}

func NewNull() Value {
	return Value{
		Type: Type{TypeID: TypeIDNull},
	}
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
	var t *Type
	if len(value) > 0 {
		curT := value[0].Type
		t = &curT
	}
	return Value{
		Type: Type{TypeID: TypeIDList, List: struct{ Element *Type }{Element: t}},
		List: value,
	}
}

func NewStruct(names []string, value []Value) Value {
	// Fixme performance.
	fieldTypes := make([]StructField, len(value))
	for i := range value {
		fieldTypes[i] = StructField{
			Name: names[i],
			Type: value[i].Type,
		}
	}
	return Value{
		Type:        Type{TypeID: TypeIDStruct, Struct: struct{ Fields []StructField }{Fields: fieldTypes}},
		FieldValues: value,
	}
}

func NewTuple(values []Value) Value {
	types := make([]Type, len(values))
	for i := range values {
		types[i] = values[i].Type
	}
	return Value{
		Type:  Type{TypeID: TypeIDTuple, Tuple: struct{ Elements []Type }{Elements: types}},
		Tuple: values,
	}
}

func (value Value) Compare(other Value) int {
	// The runtime types may be different for a union.
	// The concrete instance type will be present.
	if value.Type.TypeID != other.Type.TypeID {
		if value.Type.TypeID < other.Type.TypeID {
			return -1
		} else {
			return 1
		}
	}

	switch value.Type.TypeID {
	case TypeIDNull:
		return 0

	case TypeIDInt:
		if value.Int < other.Int {
			return -1
		} else if value.Int > other.Int {
			return 1
		} else {
			return 0
		}

	case TypeIDFloat:
		if value.Float < other.Float {
			return -1
		} else if value.Float > other.Float {
			return 1
		} else {
			return 0
		}

	case TypeIDBoolean:
		if value.Boolean == other.Boolean {
			return 0
		} else if !value.Boolean {
			return -1
		} else {
			return 1
		}

	case TypeIDString:
		left := strings.ToLower(value.Str)
		right := strings.ToLower(other.Str)
		if left < right {
			return -1
		} else if left > right {
			return 1
		} else {
			// Here we reverse the ordering, cause Go would want upper-case letters to go first, we want lower-case letters first.
			if value.Str < other.Str {
				return 1
			} else if value.Str > other.Str {
				return -1
			} else {
				return 0
			}
		}

	case TypeIDTime:
		if value.Time.Before(other.Time) {
			return -1
		} else if value.Time.After(other.Time) {
			return 1
		} else {
			return 0
		}

	case TypeIDDuration:
		if value.Duration < other.Duration {
			return -1
		} else if value.Duration > other.Duration {
			return 1
		} else {
			return 0
		}

	case TypeIDList:
		maxLen := len(value.List)
		if len(other.List) > maxLen {
			maxLen = len(other.List)
		}

		for i := 0; i < maxLen; i++ {
			if i == len(value.List) {
				return -1
			} else if i == len(other.List) {
				return 1
			}

			if comp := value.List[i].Compare(other.List[i]); comp != 0 {
				return comp
			}
		}

		return 0

	case TypeIDStruct:
		maxLen := len(value.FieldValues)
		if len(other.FieldValues) > maxLen {
			maxLen = len(other.FieldValues)
		}

		for i := 0; i < maxLen; i++ {
			if i == len(value.FieldValues) {
				return -1
			} else if i == len(other.FieldValues) {
				return 1
			}

			if comp := value.FieldValues[i].Compare(other.FieldValues[i]); comp != 0 {
				return comp
			}
		}

		return 0

	case TypeIDTuple:
		maxLen := len(value.Tuple)
		if len(other.Tuple) > maxLen {
			maxLen = len(other.Tuple)
		}

		for i := 0; i < maxLen; i++ {
			if i == len(value.Tuple) {
				return -1
			} else if i == len(other.Tuple) {
				return 1
			}

			if comp := value.Tuple[i].Compare(other.Tuple[i]); comp != 0 {
				return comp
			}
		}

		return 0

	case TypeIDUnion:
		panic("can't have union type as concrete value instance")
	default:
		panic("impossible, type switch bug")
	}
}

func (value Value) String() string {
	builder := &strings.Builder{}
	value.append(builder)
	return builder.String()
}

func (value Value) append(builder *strings.Builder) {
	switch value.Type.TypeID {
	case TypeIDNull:
		builder.WriteString("<null>")

	case TypeIDInt:
		builder.WriteString(fmt.Sprint(value.Int))

	case TypeIDFloat:
		builder.WriteString(fmt.Sprint(value.Float))

	case TypeIDBoolean:
		builder.WriteString(fmt.Sprint(value.Boolean))

	case TypeIDString:
		builder.WriteString(fmt.Sprintf("'%s'", value.Str))

	case TypeIDTime:
		builder.WriteString(value.Time.Format(time.RFC3339))

	case TypeIDDuration:
		builder.WriteString(fmt.Sprint(value.Duration))

	case TypeIDList:
		builder.WriteString("[")
		for i, v := range value.List {
			v.append(builder)
			if i != len(value.List)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString("]")

	case TypeIDStruct:
		builder.WriteString("{ ")
		for i, v := range value.FieldValues {
			builder.WriteString(value.Type.Struct.Fields[i].Name)
			builder.WriteString(": ")
			v.append(builder)
			if i != len(value.FieldValues)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(" }")

	case TypeIDTuple:
		builder.WriteString("(")
		for i, v := range value.Tuple {
			v.append(builder)
			if i != len(value.Tuple)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(")")

	case TypeIDUnion:
		panic("can't have union type as concrete value instance")
	default:
		panic("impossible, type switch bug")
	}
}

func (value Value) ToRawGoValue() interface{} {
	switch value.Type.TypeID {
	case TypeIDNull:
		return nil
	case TypeIDInt:
		return value.Int
	case TypeIDFloat:
		return value.Float
	case TypeIDBoolean:
		return value.Boolean
	case TypeIDString:
		return value.Str
	case TypeIDTime:
		return value.Time
	case TypeIDDuration:
		return value.Duration
	default:
		panic("invalid octosql.Value to get Raw Go value for")
	}
}

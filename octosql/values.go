package octosql

import (
	"fmt"
	"strings"
	"time"
)

var ZeroValue = Value{}

// Value represents a single row value. The zero value of it is conveniently NULL.
type Value struct {
	TypeID   TypeID
	Int      int
	Float    float64
	Boolean  bool
	Str      string
	Time     time.Time
	Duration time.Duration
	List     []Value
	Struct   []Value
	Tuple    []Value
}

func NewNull() Value {
	return Value{
		TypeID: TypeIDNull,
	}
}

func NewInt(value int) Value {
	return Value{
		TypeID: TypeIDInt,
		Int:    value,
	}
}

func NewFloat(value float64) Value {
	return Value{
		TypeID: TypeIDFloat,
		Float:  value,
	}
}

func NewBoolean(value bool) Value {
	return Value{
		TypeID:  TypeIDBoolean,
		Boolean: value,
	}
}

func NewString(value string) Value {
	return Value{
		TypeID: TypeIDString,
		Str:    value,
	}
}

func NewTime(value time.Time) Value {
	return Value{
		TypeID: TypeIDTime,
		Time:   value,
	}
}

func NewDuration(value time.Duration) Value {
	return Value{
		TypeID:   TypeIDDuration,
		Duration: value,
	}
}

func NewList(value []Value) Value {
	return Value{
		TypeID: TypeIDList,
		List:   value,
	}
}

func NewStruct(value []Value) Value {
	return Value{
		TypeID: TypeIDStruct,
		Struct: value,
	}
}

func NewTuple(values []Value) Value {
	return Value{
		TypeID: TypeIDTuple,
		Tuple:  values,
	}
}

func (value Value) Compare(other Value) int {
	// The runtime types may be different for a union.
	// The concrete instance type will be present.
	if value.TypeID != other.TypeID {
		if value.TypeID < other.TypeID {
			return -1
		} else {
			return 1
		}
	}

	switch value.TypeID {
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
		maxLen := len(value.Struct)
		if len(other.Struct) > maxLen {
			maxLen = len(other.Struct)
		}

		for i := 0; i < maxLen; i++ {
			if i == len(value.Struct) {
				return -1
			} else if i == len(other.Struct) {
				return 1
			}

			if comp := value.Struct[i].Compare(other.Struct[i]); comp != 0 {
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

func (value Value) Equal(other Value) bool {
	if value.TypeID == TypeIDNull && other.TypeID == TypeIDNull {
		return false
	}
	return value.Compare(other) == 0
}

func (value Value) Type() Type {
	switch value.TypeID {
	case TypeIDList:
		var element *Type
		for i := range value.List {
			if element == nil {
				t := value.List[i].Type()
				element = &t
			} else {
				t := TypeSum(*element, value.List[i].Type())
				element = &t
			}
		}
		return Type{
			TypeID: TypeIDList,
			List:   struct{ Element *Type }{Element: element},
		}

	case TypeIDStruct:
		// TODO: A type registry and a reference to a struct type would be useful here for field names.
		fields := make([]StructField, len(value.Struct))
		for i := range value.Tuple {
			fields[i].Type = value.Struct[i].Type()
		}
		return Type{
			TypeID: TypeIDStruct,
			Struct: struct{ Fields []StructField }{Fields: fields},
		}

	case TypeIDTuple:
		elements := make([]Type, len(value.Tuple))
		for i := range value.Tuple {
			elements[i] = value.Tuple[i].Type()
		}
		return Type{
			TypeID: TypeIDTuple,
			Tuple:  struct{ Elements []Type }{Elements: elements},
		}
	}

	return Type{
		TypeID: value.TypeID,
	}
}

func (value Value) String() string {
	builder := &strings.Builder{}
	value.append(builder)
	return builder.String()
}

func (value Value) append(builder *strings.Builder) {
	switch value.TypeID {
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
		for i, v := range value.Struct {
			// TODO: This method should receive type information for proper name display.
			// builder.WriteString(value.Type.Struct.Fields[i].Name)
			// builder.WriteString(": ")
			v.append(builder)
			if i != len(value.Struct)-1 {
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
	// TODO: Add complex types.
	switch value.TypeID {
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

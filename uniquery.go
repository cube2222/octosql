package uniquery

import "errors"

type Datatype string

const (
	ColumnString Datatype = "string"
	ColumnInt    Datatype = "int"
)

type FieldIdentifier string

type Field struct {
	Name FieldIdentifier
	Type Datatype
}

type Value struct {
	Value interface{}
}

type Record interface {
	Value(field FieldIdentifier) Value
	Fields() []Field
}

type RecordStream interface {
	Next() (Record, error)
}

var ErrEndOfStream = errors.New("end of stream")

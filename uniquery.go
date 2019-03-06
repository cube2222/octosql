package octosql

import (
	"errors"
)

type Datatype string

const (
	DatatypeString Datatype = "string"
	DatatypeInt    Datatype = "int"
)

type FieldIdentifier string

type Field struct {
	Name FieldIdentifier
	Type Datatype
}

type Record struct {
	data map[string]interface{}
}

func NewRecord(data map[string]interface{}) *Record {
	return &Record{
		data: data,
	}
}

func (r *Record) Value(field FieldIdentifier) interface{} {
	return r.data[string(field)]
}

func (r *Record) Fields() []Field {
	fields := make([]Field, 0)
	for k := range r.data {
		fields = append(fields, Field{
			Name: FieldIdentifier(k),
			Type: getType(r.data[k]),
		})
	}

	return fields
}

func getType(i interface{}) Datatype {
	return DatatypeString
}

type RecordStream interface {
	Next() (*Record, error)
}

var ErrEndOfStream = errors.New("end of stream")

package execution

import (
	"io"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Field struct {
	Name octosql.VariableName
}

type Record struct {
	fieldNames []octosql.VariableName
	data       []interface{}
}

func NewRecord(fields []octosql.VariableName, data map[octosql.VariableName]interface{}) *Record {
	dataInner := make([]interface{}, len(fields))
	for i := range fields {
		dataInner[i] = data[fields[i]]
	}
	return &Record{
		fieldNames: fields,
		data:       dataInner,
	}
}

func (r *Record) Value(field octosql.VariableName) interface{} {
	for i := range r.fieldNames {
		if r.fieldNames[i] == field {
			return r.data[i]
		}
	}
	return nil
}

func (r *Record) Fields() []Field {
	fields := make([]Field, 0)
	for _, fieldName := range r.fieldNames {
		fields = append(fields, Field{
			Name: fieldName,
		})
	}

	return fields
}

func (r *Record) AsVariables() octosql.Variables {
	out := make(octosql.Variables)
	for i := range r.fieldNames {
		out[r.fieldNames[i]] = r.data[i]
	}

	return out
}

type RecordStream interface {
	Next() (*Record, error)
	io.Closer
}

var ErrEndOfStream = errors.New("end of stream")

var ErrNotFound = errors.New("not found")

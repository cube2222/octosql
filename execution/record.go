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
	data       []octosql.Value
}

func NewRecord(fields []octosql.VariableName, data map[octosql.VariableName]octosql.Value) *Record {
	dataInner := make([]octosql.Value, len(fields))
	for i := range fields {
		dataInner[i] = data[fields[i]]
	}
	return &Record{
		fieldNames: fields,
		data:       dataInner,
	}
}

func (r *Record) Value(field octosql.VariableName) octosql.Value {
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

func (r *Record) AsTuple() octosql.Tuple {
	return octosql.MakeTuple(r.data)
}

func (r *Record) Equal(other *Record) bool {
	myFields := r.Fields()
	otherFields := other.Fields()
	if len(myFields) != len(otherFields) {
		return false
	}

	for i := range myFields {
		if myFields[i] != otherFields[i] {
			return false
		}
		if !octosql.AreEqual(r.data[i], other.data[i]) {
			return false
		}
	}
	return true
}

type RecordStream interface {
	Next() (*Record, error)
	io.Closer
}

var ErrEndOfStream = errors.New("end of stream")

var ErrNotFound = errors.New("not found")

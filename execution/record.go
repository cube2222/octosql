package execution

import (
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

func getType(i interface{}) octosql.Datatype {
	if _, ok := i.(int); ok {
		return octosql.DatatypeInt
	}
	if _, ok := i.(float32); ok {
		return octosql.DatatypeFloat32
	}
	if _, ok := i.(float64); ok {
		return octosql.DatatypeFloat64
	}
	if _, ok := i.(string); ok {
		return octosql.DatatypeString
	}
	return octosql.DatatypeString // TODO: Unknown
}

type RecordStream interface {
	Next() (*Record, error)
	Close() error
}

var ErrEndOfStream = errors.New("end of stream")

var ErrNotFound = errors.New("not found")

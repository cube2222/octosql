package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Field struct {
	Name octosql.VariableName
	Type octosql.Datatype
}

type Record struct {
	fieldNames []octosql.VariableName
	data       map[octosql.VariableName]interface{}
}

func NewRecord(fields []octosql.VariableName, data map[octosql.VariableName]interface{}) *Record {
	return &Record{
		fieldNames: fields,
		data:       data,
	}
}

func (r *Record) Value(field octosql.VariableName) interface{} {
	return r.data[field]
}

func (r *Record) Fields() []Field {
	fields := make([]Field, 0)
	for _, fieldName := range r.fieldNames {
		fields = append(fields, Field{
			Name: fieldName,
			Type: getType(r.data[fieldName]),
		})
	}

	return fields
}

func (r *Record) AsVariables() octosql.Variables {
	out := make(octosql.Variables)
	for k, v := range r.data {
		out[k] = v
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
}

var ErrEndOfStream = errors.New("end of stream")

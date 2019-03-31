package execution

import "github.com/cube2222/octosql"

type InMemoryStream struct {
	fields []octosql.VariableName
	data   []map[octosql.VariableName]interface{}
	index  int
}

func NewInMemoryStream(fields []octosql.VariableName, data []map[octosql.VariableName]interface{}) *InMemoryStream {
	return &InMemoryStream{
		fields: fields,
		data:   data,
		index:  0,
	}
}

func (ims *InMemoryStream) Next() (*Record, error) {
	if ims.index >= len(ims.data) {
		return nil, ErrEndOfStream
	}

	recData := ims.data[ims.index]
	ims.index++

	return NewRecord(ims.fields, recData), nil
}

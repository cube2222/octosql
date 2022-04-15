package formats

import (
	"io"

	"github.com/segmentio/encoding/json"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type JSONFormatter struct {
	encoder *json.Encoder
	fields  []physical.SchemaField
}

func NewJSONFormatter(w io.Writer) *JSONFormatter {
	encoder := json.NewEncoder(w)

	return &JSONFormatter{
		encoder: encoder,
	}
}

func (t *JSONFormatter) SetSchema(schema physical.Schema) {
	t.fields = schema.Fields
}

func (t *JSONFormatter) Write(values []octosql.Value) error {
	out := make(map[string]interface{}, len(values))
	for i := range t.fields {
		out[t.fields[i].Name] = values[i].ToRawGoValue(t.fields[i].Type)
	}

	return t.encoder.Encode(out)
}

func (t *JSONFormatter) Close() error {
	return nil
}

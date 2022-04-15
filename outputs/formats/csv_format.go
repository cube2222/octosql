package formats

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type CSVFormatter struct {
	writer *csv.Writer
	fields []physical.SchemaField
}

func NewCSVFormatter(w io.Writer) *CSVFormatter {
	writer := csv.NewWriter(w)

	return &CSVFormatter{
		writer: writer,
	}
}

func (t *CSVFormatter) SetSchema(schema physical.Schema) {
	t.fields = schema.Fields

	header := make([]string, len(schema.Fields))
	for i := range schema.Fields {
		header[i] = schema.Fields[i].Name
	}
	t.writer.Write(header)
}

func (t *CSVFormatter) Write(values []octosql.Value) error {
	row := make([]string, len(values))
	for i := range values {
		row[i] = fmt.Sprintf("%v", values[i].ToRawGoValue(t.fields[i].Type))
	}
	return t.writer.Write(row)
}

func (t *CSVFormatter) Close() error {
	t.writer.Flush()
	return nil
}

package batch

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type CSVFormatter struct {
	writer *csv.Writer
}

func NewCSVFormatter(w io.Writer) Format {
	writer := csv.NewWriter(w)

	return &CSVFormatter{
		writer: writer,
	}
}

func (t *CSVFormatter) SetSchema(schema physical.Schema) {
	header := make([]string, len(schema.Fields))
	for i := range schema.Fields {
		header[i] = schema.Fields[i].Name
	}
	t.writer.Write(header)
}

func (t *CSVFormatter) Write(values []octosql.Value) error {
	row := make([]string, len(values))
	for i := range values {
		row[i] = fmt.Sprintf("%v", values[i].ToRawGoValue())
	}
	return t.writer.Write(row)
}

func (t *CSVFormatter) Close() error {
	t.writer.Flush()
	return nil
}

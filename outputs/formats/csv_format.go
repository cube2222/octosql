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
	t.fields = WithoutQualifiers(schema.Fields)

	header := make([]string, len(t.fields))
	for i := range t.fields {
		header[i] = t.fields[i].Name
	}
	t.writer.Write(header)
}

func (t *CSVFormatter) Write(values [][]octosql.Value, count int) error {
	for rowIndex := 0; rowIndex < count; rowIndex++ {
		row := make([]string, len(values))
		for i := range values {
			row[i] = fmt.Sprintf("%v", values[i][rowIndex].ToRawGoValue(t.fields[i].Type))
		}
		if err := t.writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (t *CSVFormatter) Close() error {
	t.writer.Flush()
	return nil
}

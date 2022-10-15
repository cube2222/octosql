package formats

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

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

func (t *CSVFormatter) Write(values []octosql.Value) error {
	var builder strings.Builder
	row := make([]string, len(values))
	for i := range values {
		FormatCSVValue(&builder, values[i])
		row[i] = builder.String()
		builder.Reset()
	}
	return t.writer.Write(row)
}

func FormatCSVValue(builder *strings.Builder, value octosql.Value) {
	switch value.TypeID {
	case octosql.TypeIDNull:
	case octosql.TypeIDInt:
		builder.WriteString(strconv.FormatInt(int64(value.Int), 10))
	case octosql.TypeIDFloat:
		builder.WriteString(strconv.FormatFloat(value.Float, 'f', -1, 64))
	case octosql.TypeIDBoolean:
		builder.WriteString(strconv.FormatBool(value.Boolean))
	case octosql.TypeIDString:
		builder.WriteString(value.Str)
	case octosql.TypeIDTime:
		builder.WriteString(value.Time.Format(time.RFC3339))
	case octosql.TypeIDDuration:
		builder.WriteString(fmt.Sprint(value.Duration))
	default:
		panic("invalid value type to print in CSV: " + value.TypeID.String())
	}
}

func (t *CSVFormatter) Close() error {
	t.writer.Flush()
	return nil
}

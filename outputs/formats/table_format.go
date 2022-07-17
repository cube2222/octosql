package formats

import (
	"io"

	"github.com/olekukonko/tablewriter"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type TableFormatter struct {
	table *tablewriter.Table
}

func NewTableFormatter(w io.Writer) *TableFormatter {
	table := tablewriter.NewWriter(w)
	table.SetColWidth(24)
	table.SetRowLine(false)

	return &TableFormatter{
		table: table,
	}
}

func (t *TableFormatter) SetSchema(schema physical.Schema) {
	fields := WithoutQualifiers(schema.Fields)
	header := make([]string, len(fields))
	for i := range fields {
		header[i] = fields[i].Name
	}
	t.table.SetHeader(header)
	t.table.SetAutoFormatHeaders(false)
}

func (t *TableFormatter) Write(values []octosql.Value) error {
	row := make([]string, len(values))
	for i := range values {
		row[i] = values[i].String()
	}
	t.table.Append(row)
	return nil
}

func (t *TableFormatter) Close() error {
	t.table.Render()
	return nil
}

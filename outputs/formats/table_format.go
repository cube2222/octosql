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
	header := make([]string, len(schema.Fields))
	for i := range schema.Fields {
		header[i] = schema.Fields[i].Name
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

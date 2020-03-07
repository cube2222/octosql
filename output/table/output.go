package table

import (
	"io"

	"github.com/olekukonko/tablewriter"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output"
)

type Output struct {
	w        io.Writer
	rowLines bool
	records  []*execution.Record
}

func NewOutput(w io.Writer, rowLines bool) output.Output {
	return &Output{
		w:        w,
		rowLines: rowLines,
	}
}

func (o *Output) WriteRecord(record *execution.Record) error {
	o.records = append(o.records, record)
	return nil
}

func (o *Output) Close() error {
	fieldMap := output.FindFieldsCount(o.records)
	fields := make([]string, 0)

	for field, count := range fieldMap {
		fieldName := field.Name.String()
		for i := 0; i < count; i++ {
			fields = append(fields, fieldName)
		}
	}

	table := tablewriter.NewWriter(o.w)
	table.SetRowLine(o.rowLines)
	table.SetHeader(fields)
	table.SetAutoFormatHeaders(false)

	for _, record := range o.records {
		var out []string
		for _, field := range fields {
			value := record.Value(octosql.NewVariableName(field))
			out = append(out, value.Show())
		}
		table.Append(out)
	}

	table.Render()

	return nil
}

package table

import (
	"fmt"
	"io"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output"
	"github.com/olekukonko/tablewriter"
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
	var fields []string
	for _, record := range o.records {
		for _, field := range record.Fields() {
			found := false
			for i := range fields {
				if fields[i] == field.Name.String() {
					found = true
				}
			}
			if !found {
				fields = append(fields, field.Name.String())
			}
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
			out = append(out, fmt.Sprint(value))
		}
		table.Append(out)
	}

	table.Render()

	return nil
}

package table

import (
	"fmt"
	"io"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output/badger"
)

func TableFormatter(rowLines bool) badger.TableFormatter {
	return func(w io.Writer, records []*execution.Record, watermark time.Time, errToPrint error) error {
		var fields []string
		for _, record := range records {
			for _, field := range record.ShowFields() {
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

		table := tablewriter.NewWriter(w)
		table.SetColWidth(64)
		table.SetRowLine(rowLines)
		table.SetHeader(fields)
		table.SetAutoFormatHeaders(false)

		for _, record := range records {
			var out []string
			for _, field := range fields {
				value := record.Value(octosql.NewVariableName(field))
				out = append(out, value.Show())
			}
			table.Append(out)
		}

		table.Render()

		fmt.Fprintf(w, "watermark: %s\n", watermark.Format(time.RFC3339Nano))
		if errToPrint != nil {
			fmt.Fprintf(w, "error: %s\n", errToPrint)
		}

		return nil
	}
}

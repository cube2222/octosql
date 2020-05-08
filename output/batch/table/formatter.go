package table

import (
	"fmt"
	"io"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output/batch"
)

func TableFormatter(rowLines bool) batch.TableFormatter {
	return func(w io.Writer, records []*execution.Record, watermark time.Time, errToPrint error) error {
		fields := batch.GetAllFields(records)

		table := tablewriter.NewWriter(w)
		table.SetColWidth(64)
		table.SetRowLine(rowLines)
		table.SetHeader(fields)
		table.SetAutoFormatHeaders(false)

		for _, record := range records {
			var row []string
			for _, field := range fields {
				value := record.Value(octosql.NewVariableName(field))
				row = append(row, value.Show())
			}
			table.Append(row)
		}

		table.Render()

		fmt.Fprintf(w, "watermark: %s\n", watermark.Format(time.RFC3339Nano))
		if errToPrint != nil {
			fmt.Fprintf(w, "error: %s\n", errToPrint)
		}

		return nil
	}
}

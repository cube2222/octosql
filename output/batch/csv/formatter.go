package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output/batch"
)

func TableFormatter(separator rune) batch.TableFormatter {
	return func(w io.Writer, records []*execution.Record, watermark time.Time, errToPrint error) error {
		if errToPrint != nil {
			fmt.Fprintf(w, "Error: %s", errToPrint.Error())
		}
		fields := batch.GetAllFields(records)

		out := csv.NewWriter(w)
		out.Comma = separator
		err := out.Write(fields)
		if err != nil {
			return errors.Wrap(err, "couldn't write header row")
		}

		for _, record := range records {
			var row []string
			for _, field := range fields {
				value := record.Value(octosql.NewVariableName(field))
				row = append(row, value.Show())
			}
			err := out.Write(row)
			if err != nil {
				return errors.Wrap(err, "couldn't write row")
			}
		}

		out.Flush()

		return nil
	}
}

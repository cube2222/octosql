package octosql

import (
	"fmt"
	"github.com/cube2222/octosql/output"
	csvoutput "github.com/cube2222/octosql/output/csv"
	jsonoutput "github.com/cube2222/octosql/output/json"
	"github.com/cube2222/octosql/output/table"
	"os"
)

var DEFAULT_OUTPUT_FORMAT = "table"

type InvalidOutputFormatError struct {
	outputFormat string
}

func (err *InvalidOutputFormatError) Error() string {
	return fmt.Sprintf("Invalid output format: %s", err.outputFormat)
}

func translateOutputName(outputFormat string) (output.Output, error) {
	switch outputFormat {
	case "table":
		return table.NewOutput(os.Stdout, false), nil
	case "table_row_separated":
		return table.NewOutput(os.Stdout, true), nil
	case "json":
		return jsonoutput.NewOutput(os.Stdout), nil
	case "csv":
		return csvoutput.NewOutput(',', os.Stdout), nil
	case "tabbed":
		return csvoutput.NewOutput('\t', os.Stdout), nil
	default:
		return table.NewOutput(os.Stdout, false), &InvalidOutputFormatError{
			outputFormat: outputFormat,
		}
	}
}
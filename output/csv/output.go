package csv

import (
	"encoding/csv"
	"io"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output"
)

type Output struct {
	separator rune
	w         io.Writer
	records   []*execution.Record
}

func NewOutput(separator rune, w io.Writer) output.Output {
	return &Output{
		separator: separator,
		w:         w,
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

	out := csv.NewWriter(o.w)
	out.Comma = o.separator
	err := out.Write(fields)
	if err != nil {
		return errors.Wrap(err, "couldn't write header row")
	}

	for _, record := range o.records {
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

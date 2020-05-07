package batch

import (
	"io"
	"time"

	"github.com/cube2222/octosql/execution"
)

type TableFormatter func(w io.Writer, records []*execution.Record, watermark time.Time, err error) error

func GetAllFields(records []*execution.Record) []string {
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
	return fields
}

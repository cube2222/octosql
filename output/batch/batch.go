package batch

import (
	"io"
	"time"

	"github.com/cube2222/octosql/execution"
)

type TableFormatter func(w io.Writer, records []*execution.Record, watermark time.Time, err error) error

func GetAllFields(records []*execution.Record) []string {
	fieldsFound := make(map[string]bool)
	var fields []string
	for _, record := range records {
		for _, field := range record.ShowFields() {
			if fieldName := field.Name.String(); !fieldsFound[fieldName] {
				fieldsFound[fieldName] = true
				fields = append(fields, field.Name.String())
			}
		}
	}
	return fields
}

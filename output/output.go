package output

import (
	"io"

	"github.com/cube2222/octosql/execution"
)

type Output interface {
	WriteRecord(record *execution.Record) error
	io.Closer
}

func FindFieldsCount(records []*execution.Record) map[execution.Field]int {
	fieldMap := make(map[execution.Field]int)
	for _, record := range records {
		currentRecordFieldMap := make(map[execution.Field]int)
		recordFields := record.ShowFields()

		for _, field := range recordFields {
			count, ok := currentRecordFieldMap[field]

			if !ok {
				currentRecordFieldMap[field] = 1
			} else {
				currentRecordFieldMap[field] = count + 1
			}

			_, ok = fieldMap[field]
			if !ok {
				fieldMap[field] = 0
			}
		}

		for _, field := range recordFields {
			if currentRecordFieldMap[field] > fieldMap[field] {
				fieldMap[field] = currentRecordFieldMap[field]
			}
		}
	}

	return fieldMap
}

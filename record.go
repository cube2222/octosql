package octosql

import "github.com/apache/arrow/go/arrow/array"

type Record array.Record

func OctoRecord(record array.Record) *Record {
	rec := Record(record)
	return &rec
}

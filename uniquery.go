package uniquery

import "context"

type ColumnType string

const (
	ColumnString = "string"
	ColumnInt    = "int"
)

type Column struct {
	Name string
	Type ColumnType
}

type Value struct {
	Value interface{}
}

type Record interface {
	Value(column string) Value
	Columns() []Column
}

type RecordGetter interface {
	Next() (Record, error)
}

type GetAll interface {
	GetAll(ctx context.Context, table string) (RecordGetter, error)
}

type GetWherePrimary interface {
	GetWherePrimary(ctx context.Context, table string, key interface{}) (RecordGetter, error)
}

type Sizer interface {
	Size() int64
}

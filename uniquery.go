package uniquery

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

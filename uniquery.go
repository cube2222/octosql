package uniquery

type Datatype string

const (
	ColumnString Datatype = "string"
	ColumnInt    Datatype = "int"
)

type Field struct {
	Name string
	Type Datatype
}

type Value struct {
	Value interface{}
}

type Record interface {
	Value(field string) Value
	Fields() []Field
}

type RecordStream interface {
	Next() (Record, error)
}

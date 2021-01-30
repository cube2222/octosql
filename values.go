package octosql

import "time"

var ZeroValue = Value{}

type Value struct {
	Type        Type
	Int         int
	Float       float64
	Boolean     bool
	Str         string
	Time        time.Time
	Duration    time.Duration
	List        []Value
	FieldValues []Value
}

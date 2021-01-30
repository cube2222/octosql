package octosql

import "time"

type Value struct {
	Type        Type
	Int         int
	Float       float64
	Boolean     bool
	String      string
	Time        time.Time
	Duration    time.Duration
	List        []Value
	FieldValues []Value
}

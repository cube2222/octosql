package aggregate

import "github.com/cube2222/octosql/execution"

var AggregateTable = map[string]execution.AggregatePrototype{
	"count": func() execution.Aggregate { return &Count{} },
	"sum":   func() execution.Aggregate { return &Sum{} },
}

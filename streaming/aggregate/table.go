package aggregate

import "github.com/cube2222/octosql/execution"

var AggregateTable = map[string]execution.AggregatePrototype{
	"count":    func() execution.Aggregate { return NewCountAggregate() },
	"sum":      func() execution.Aggregate { return NewSumAggregate() },
	"avg":      func() execution.Aggregate { return NewAverageAggregate() },
	"min":      func() execution.Aggregate { return NewMinAggregate() },
	"max":      func() execution.Aggregate { return NewMaxAggregate() },
	"distinct": func() execution.Aggregate { return NewDistinctAggregate() },
	"first":    func() execution.Aggregate { return NewFirstAggregate() },
	"last":     func() execution.Aggregate { return NewLastAggregate() },
}

package aggregates

import "github.com/cube2222/octosql/execution"

var AggregateTable = map[string]execution.AggregatePrototype{
	"count":          func() execution.Aggregate { return NewCountAggregate() },
	"sum":            func() execution.Aggregate { return NewSumAggregate() },
	"avg":            func() execution.Aggregate { return NewAverageAggregate() },
	"min":            func() execution.Aggregate { return NewMinAggregate() },
	"max":            func() execution.Aggregate { return NewMaxAggregate() },
	"sum_distinct":   func() execution.Aggregate { return NewDistinctAggregate(NewSumAggregate()) },
	"count_distinct": func() execution.Aggregate { return NewDistinctAggregate(NewCountAggregate()) },
	"avg_distinct":   func() execution.Aggregate { return NewDistinctAggregate(NewAverageAggregate()) },
	"first":          func() execution.Aggregate { return NewFirstAggregate() },
	"last":           func() execution.Aggregate { return NewLastAggregate() },
	"key":            func() execution.Aggregate { return NewKeyAggregate() },
}

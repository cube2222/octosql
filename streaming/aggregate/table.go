package aggregate

var AggregateTable = map[string]AggregatePrototype{
	"count":          func() Aggregate { return NewCountAggregate() },
	"sum":            func() Aggregate { return NewSumAggregate() },
	"avg":            func() Aggregate { return NewAverageAggregate() },
	"min":            func() Aggregate { return NewMinAggregate() },
	"max":            func() Aggregate { return NewMaxAggregate() },
	"sum_distinct":   func() Aggregate { return NewDistinctAggregate(NewSumAggregate()) },
	"count_distinct": func() Aggregate { return NewDistinctAggregate(NewCountAggregate()) },
	"avg_distinct":   func() Aggregate { return NewDistinctAggregate(NewAverageAggregate()) },
	"first":          func() Aggregate { return NewFirstAggregate() },
	"last":           func() Aggregate { return NewLastAggregate() },
	"key":            func() Aggregate { return NewKeyAggregate() },
}

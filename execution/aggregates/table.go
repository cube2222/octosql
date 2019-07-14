package aggregates

import "github.com/cube2222/octosql/execution"

var AggregateTable = map[string]execution.AggregatePrototype{
	NewAverage().String():              func() execution.Aggregate { return NewAverage() },
	NewDistinct(NewAverage()).String(): func() execution.Aggregate { return NewDistinct(NewAverage()) },
	NewCount().String():                func() execution.Aggregate { return NewCount() },
	NewDistinct(NewCount()).String():   func() execution.Aggregate { return NewDistinct(NewCount()) },
	NewFirst().String():                func() execution.Aggregate { return NewFirst() },
	NewLast().String():                 func() execution.Aggregate { return NewLast() },
	NewMax().String():                  func() execution.Aggregate { return NewMax() },
	NewMin().String():                  func() execution.Aggregate { return NewMin() },
	NewSum().String():                  func() execution.Aggregate { return NewSum() },
	NewDistinct(NewSum()).String():     func() execution.Aggregate { return NewDistinct(NewSum()) },
}

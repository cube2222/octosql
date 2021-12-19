package aggregates

import (
	"github.com/cube2222/octosql/physical"
)

var Aggregates = map[string][]physical.AggregateDescriptor{
	"array_agg":          ArrayOverloads,
	"array_agg_distinct": DistinctAggregateOverloads(ArrayOverloads),
	"count":              CountOverloads,
	"count_distinct":     DistinctAggregateOverloads(CountOverloads),
	"sum":                SumOverloads,
	"sum_distinct":       DistinctAggregateOverloads(SumOverloads),
	"avg":                AverageOverloads,
	"avg_distinct":       DistinctAggregateOverloads(AverageOverloads),
	"max":                MaxOverloads,
	"min":                MinOverloads,
}

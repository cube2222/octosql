package aggregates

import (
	"github.com/cube2222/octosql/physical"
)

var Aggregates = map[string]physical.AggregateDetails{
	"array_agg": {
		Description: "Creates an array of all items in the group.",
		Descriptors: ArrayOverloads,
	},
	"array_agg_distinct": {
		Description: "Creates an array of distinct items in the group.",
		Descriptors: DistinctAggregateOverloads(ArrayOverloads),
	},
	"count": {
		Description: "Counts all items in the group.",
		Descriptors: CountOverloads,
	},
	"count_distinct": {
		Description: "Counts distinct items in the group.",
		Descriptors: DistinctAggregateOverloads(CountOverloads),
	},
	"sum": {
		Description: "Sums all items in the group.",
		Descriptors: SumOverloads,
	},
	"sum_distinct": {
		Description: "Sums distinct items in the group.",
		Descriptors: DistinctAggregateOverloads(SumOverloads),
	},
	"avg": {
		Description: "Averages all items in the group.",
		Descriptors: AverageOverloads,
	},
	"avg_distinct": {
		Description: "Averages distinct items in the group.",
		Descriptors: DistinctAggregateOverloads(AverageOverloads),
	},
	"max": {
		Description: "Returns maximum item in the group.",
		Descriptors: MaxOverloads,
	},
	"min": {
		Description: "Returns minimum item in the group.",
		Descriptors: MinOverloads,
	},
}

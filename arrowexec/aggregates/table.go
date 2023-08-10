package aggregates

import (
	"github.com/cube2222/octosql/physical"
)

var Aggregates = map[string]physical.AggregateDetails{
	"count": {
		Description: "Counts all items in the group.",
		Descriptors: CountOverloads,
	},
}

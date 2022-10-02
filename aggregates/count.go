package aggregates

import (
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var CountOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Any,
		OutputType:   octosql.Int,
		Prototype:    NewCountPrototype(),
	},
}

type Count struct {
	count int
}

func NewCountPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &Count{
			count: 0,
		}
	}
}

func (c *Count) Add(retraction bool, value octosql.Value) bool {
	if !retraction {
		c.count++
	} else {
		c.count--
	}
	return c.count == 0
}

func (c *Count) Trigger() octosql.Value {
	return octosql.NewInt(c.count)
}

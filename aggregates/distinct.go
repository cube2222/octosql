package aggregates

import (
	"github.com/zyedidia/generic/hashmap"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func DistinctAggregateOverloads(overloads []physical.AggregateDescriptor) []physical.AggregateDescriptor {
	out := make([]physical.AggregateDescriptor, len(overloads))
	for i := range overloads {
		out[i] = physical.AggregateDescriptor{
			ArgumentType: overloads[i].ArgumentType,
			OutputType:   overloads[i].OutputType,
			TypeFn:       overloads[i].TypeFn,
			// Prototype:    NewDistinctPrototype(overloads[i].Prototype),
		}
	}
	return out
}

type Distinct struct {
	items   *hashmap.Map[octosql.Value, *distinctKey]
	wrapped nodes.Aggregate
}

func NewDistinctPrototype(wrapped func() nodes.Aggregate) func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &Distinct{
			items: hashmap.New[octosql.Value, *distinctKey](
				execution.BTreeDefaultDegree,
				func(a, b octosql.Value) bool {
					return a.Compare(b) == 0
				}, func(v octosql.Value) uint64 {
					return v.Hash()
				}),
			wrapped: wrapped(),
		}
	}
}

type distinctKey struct {
	count int
}

func (c *Distinct) Add(retraction bool, value octosql.Value) bool {
	item, ok := c.items.Get(value)
	if !ok {
		item = &distinctKey{count: 0}
		c.items.Put(value, item)
	}
	if !retraction {
		item.count++
	} else {
		item.count--
	}
	if item.count == 1 && !retraction {
		c.wrapped.Add(false, value)
	} else if item.count == 0 {
		c.items.Remove(value)
		c.wrapped.Add(true, value)
	}
	return c.items.Size() == 0
}

func (c *Distinct) Trigger() octosql.Value {
	return c.wrapped.Trigger()
}

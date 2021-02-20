package aggregates

import (
	"fmt"

	"github.com/google/btree"

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
			Prototype:    NewDistinctPrototype(overloads[i].Prototype),
		}
	}
	return out
}

type Distinct struct {
	items   *btree.BTree
	wrapped nodes.Aggregate
}

func NewDistinctPrototype(wrapped func() nodes.Aggregate) func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &Distinct{
			items:   btree.New(execution.BTreeDefaultDegree),
			wrapped: wrapped(),
		}
	}
}

type distinctKey struct {
	value octosql.Value
	count int
}

func (key *distinctKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(*distinctKey)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	return key.value.Compare(thanTyped.value) == -1
}

func (c *Distinct) Add(retraction bool, value octosql.Value) bool {
	item := c.items.Get(&distinctKey{value: value})
	var itemTyped *distinctKey

	if item == nil {
		itemTyped = &distinctKey{value: value, count: 0}
		c.items.ReplaceOrInsert(itemTyped)
	} else {
		var ok bool
		itemTyped, ok = item.(*distinctKey)
		if !ok {
			panic(fmt.Sprintf("invalid received item: %v", item))
		}
	}
	if !retraction {
		itemTyped.count++
	} else {
		itemTyped.count--
	}
	if itemTyped.count == 1 && !retraction {
		c.wrapped.Add(false, value)
	} else if itemTyped.count == 0 {
		c.items.Delete(itemTyped)
		c.wrapped.Add(true, value)
	}
	return c.items.Len() == 0
}

func (c *Distinct) Trigger() octosql.Value {
	return c.wrapped.Trigger()
}

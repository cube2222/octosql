package aggregates

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var MinOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Int,
		OutputType:   octosql.Int,
		Prototype:    NewMinPrototype(),
	},
	{
		ArgumentType: octosql.Float,
		OutputType:   octosql.Float,
		Prototype:    NewMinPrototype(),
	},
	{
		ArgumentType: octosql.Duration,
		OutputType:   octosql.Duration,
		Prototype:    NewMinPrototype(),
	},
}

type Min struct {
	items *btree.BTree
}

func NewMinPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &Min{
			items: btree.New(execution.BTreeDefaultDegree),
		}
	}
}

type minKey struct {
	value octosql.Value
	count int
}

func (key *minKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(*minKey)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	return key.value.Compare(thanTyped.value) == -1
}

func (c *Min) Add(retraction bool, value octosql.Value) bool {
	item := c.items.Get(&minKey{value: value})
	var itemTyped *minKey

	if item == nil {
		itemTyped = &minKey{value: value, count: 0}
		c.items.ReplaceOrInsert(itemTyped)
	} else {
		var ok bool
		itemTyped, ok = item.(*minKey)
		if !ok {
			panic(fmt.Sprintf("invalid received item: %v", item))
		}
	}
	if !retraction {
		itemTyped.count++
	} else {
		itemTyped.count--
	}
	if itemTyped.count == 0 {
		c.items.Delete(itemTyped)
	}
	return c.items.Len() == 0
}

func (c *Min) Trigger() octosql.Value {
	return c.items.Min().(*minKey).value
}

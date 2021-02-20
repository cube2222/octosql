package aggregates

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var MaxOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Int,
		OutputType:   octosql.Int,
		Prototype:    NewMaxPrototype(),
	},
	{
		ArgumentType: octosql.Float,
		OutputType:   octosql.Float,
		Prototype:    NewMaxPrototype(),
	},
	{
		ArgumentType: octosql.Duration,
		OutputType:   octosql.Duration,
		Prototype:    NewMaxPrototype(),
	},
}

type Max struct {
	items *btree.BTree
}

func NewMaxPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &Max{
			items: btree.New(execution.BTreeDefaultDegree),
		}
	}
}

type maxKey struct {
	value octosql.Value
	count int
}

func (key *maxKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(*maxKey)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	return key.value.Compare(thanTyped.value) == -1
}

func (c *Max) Add(retraction bool, value octosql.Value) bool {
	item := c.items.Get(&maxKey{value: value})
	var itemTyped *maxKey

	if item == nil {
		itemTyped = &maxKey{value: value, count: 0}
		c.items.ReplaceOrInsert(itemTyped)
	} else {
		var ok bool
		itemTyped, ok = item.(*maxKey)
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

func (c *Max) Trigger() octosql.Value {
	return c.items.Max().(*maxKey).value
}

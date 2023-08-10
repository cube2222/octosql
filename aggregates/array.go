package aggregates

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var ArrayOverloads = []physical.AggregateDescriptor{
	{
		TypeFn: func(t octosql.Type) (octosql.Type, bool) {
			return octosql.Type{TypeID: octosql.TypeIDList, List: struct{ Element *octosql.Type }{Element: &t}}, true
		},
		// Prototype: NewArrayPrototype(),
	},
}

// TODO: Elements should be sorted as they come, not sorted by value in a BTree.
type Array struct {
	items *btree.BTree
}

func NewArrayPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &Array{
			items: btree.New(execution.BTreeDefaultDegree),
		}
	}
}

type arrayKey struct {
	value octosql.Value
	count int
}

func (key *arrayKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(*arrayKey)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	return key.value.Compare(thanTyped.value) == -1
}

func (c *Array) Add(retraction bool, value octosql.Value) bool {
	item := c.items.Get(&arrayKey{value: value})
	var itemTyped *arrayKey

	if item == nil {
		itemTyped = &arrayKey{value: value, count: 0}
		c.items.ReplaceOrInsert(itemTyped)
	} else {
		var ok bool
		itemTyped, ok = item.(*arrayKey)
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

func (c *Array) Trigger() octosql.Value {
	out := make([]octosql.Value, 0, c.items.Len())
	c.items.Ascend(func(item btree.Item) bool {
		itemTyped, ok := item.(*arrayKey)
		if !ok {
			panic(fmt.Sprintf("invalid received item: %v", item))
		}
		for i := 0; i < itemTyped.count; i++ {
			out = append(out, itemTyped.value)
		}
		return true
	})

	return octosql.NewList(out)
}

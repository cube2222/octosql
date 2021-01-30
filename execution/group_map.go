package execution

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql"
)

type GroupKey []octosql.Value

func (key GroupKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(GroupKey)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	maxLen := len(key)
	if len(thanTyped) > maxLen {
		maxLen = len(thanTyped)
	}

	for i := 0; i < maxLen; i++ {
		if i == len(key) {
			return true
		} else if i == len(thanTyped) {
			return false
		}

		if comp := key[i].Compare(thanTyped[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

type GroupTree struct {
	btree *btree.BTree
}

func NewGroupTree() *GroupTree {
	return &GroupTree{
		btree: btree.New(12),
	}
}

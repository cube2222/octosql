package execution

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql/octosql"
)

type GroupKey []octosql.Value

func GroupKeyGenericLess(key, thanKey []octosql.Value) bool {
	maxLen := len(key)
	if len(thanKey) > maxLen {
		maxLen = len(thanKey)
	}

	for i := 0; i < maxLen; i++ {
		if i == len(key) {
			return true
		} else if i == len(thanKey) {
			return false
		}

		if comp := key[i].Compare(thanKey[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

type GroupKeyIface interface {
	GetGroupKey() GroupKey
}

func (key GroupKey) GetGroupKey() GroupKey {
	return key
}

func (key GroupKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(GroupKeyIface)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	thanKey := thanTyped.GetGroupKey()

	maxLen := len(key)
	if len(thanKey) > maxLen {
		maxLen = len(thanKey)
	}

	for i := 0; i < maxLen; i++ {
		if i == len(key) {
			return true
		} else if i == len(thanKey) {
			return false
		}

		if comp := key[i].Compare(thanKey[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

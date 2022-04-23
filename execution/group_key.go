package execution

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql/octosql"
)

type GroupKey []octosql.Value

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

	return CompareValueSlices(key, thanKey)
}

func CompareValueSlices(key, than []octosql.Value) bool {
	maxLen := len(key)
	if len(than) > maxLen {
		maxLen = len(than)
	}

	for i := 0; i < maxLen; i++ {
		if i == len(key) {
			return true
		} else if i == len(than) {
			return false
		}

		if comp := key[i].Compare(than[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

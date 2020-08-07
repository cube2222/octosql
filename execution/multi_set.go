package execution

import (
	"sync"

	"github.com/google/btree"

	"github.com/cube2222/octosql/storage"
)

var sets = sync.Map{}

type MultiSet struct {
	tree *btree.BTree
}

type keyCount struct {
	key   *Record
	count int
}

func (k *keyCount) Less(than btree.Item) bool {
	other, ok := than.(*keyCount)
	if !ok {
		return true
	}

	return k.key.Compare(other.key) == -1
}

func NewMultiSet(tx storage.StateTransaction) *MultiSet {
	tree := btree.New(2)

	actualMap, _ := sets.LoadOrStore(tx.Prefix(), tree)

	return &MultiSet{
		tree: actualMap.(*btree.BTree),
	}
}

func (set *MultiSet) GetCount(value *Record) (int, error) {
	out := set.tree.Get(&keyCount{
		key: value,
	})
	if out == nil {
		return 0, nil
	}

	return out.(*keyCount).count, nil
}

func (set *MultiSet) Insert(value *Record) error {
	out := set.tree.Get(&keyCount{
		key: value,
	})
	if out == nil {
		out = &keyCount{
			key:   value,
			count: 0,
		}
	}
	out.(*keyCount).count++

	set.tree.ReplaceOrInsert(out)

	return nil
}

func (set *MultiSet) Erase(value *Record) error {
	out := set.tree.Get(&keyCount{
		key: value,
	})
	if out == nil {
		out = &keyCount{
			key:   value,
			count: 0,
		}
	}
	count := out.(*keyCount).count
	count--

	if count == 0 {
		set.tree.Delete(&keyCount{
			key: value,
		})
	} else {
		set.tree.ReplaceOrInsert(&keyCount{
			key:   value,
			count: count,
		})
	}

	return nil
}

func (set *MultiSet) Contains(value *Record) (bool, error) {
	return set.tree.Has(&keyCount{
		key: value,
	}), nil
}

func (set *MultiSet) Clear() error {
	*set.tree = *btree.New(2)
	return nil
}

func (set *MultiSet) ReadAll() ([]*Record, error) {
	var items []*Record
	set.tree.Ascend(func(item btree.Item) bool {
		value := item.(*keyCount).key

		items = append(items, value)
		return true
	})

	return items, nil
}

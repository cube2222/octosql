package storage

import (
	"bytes"
	"sync"

	"github.com/google/btree"

	"github.com/cube2222/octosql"
)

type MultiSet struct {
	internal *sortedMap
}

type keyCount struct {
	key   []byte
	count int
}

func (k *keyCount) Less(than btree.Item) bool {
	other, ok := than.(*keyCount)
	if !ok {
		return true
	}

	return bytes.Compare(k.key, other.key) == -1
}

func NewMultiSet(tx StateTransaction) *MultiSet {
	tree := btree.New(2)

	newSortedMap := &sortedMap{
		Mutex: sync.Mutex{},
		tree:  tree,
	}

	actualMap, _ := maps.LoadOrStore(tx.Prefix(), newSortedMap)

	return &MultiSet{
		internal: actualMap.(*sortedMap),
	}
}

func (set *MultiSet) GetCount(value octosql.Value) (int, error) {
	set.internal.Lock()
	defer set.internal.Unlock()

	out := set.internal.tree.Get(&keyCount{
		key: value.MonotonicMarshal(),
	})
	if out == nil {
		return 0, nil
	}

	return out.(*keyCount).count, nil
}

func (set *MultiSet) Insert(value octosql.Value) error {
	set.internal.Lock()
	defer set.internal.Unlock()

	out := set.internal.tree.Get(&keyCount{
		key: value.MonotonicMarshal(),
	})
	if out == nil {
		out = &keyCount{
			key:   value.MonotonicMarshal(),
			count: 0,
		}
	}
	out.(*keyCount).count++

	set.internal.tree.ReplaceOrInsert(out)

	return nil
}

func (set *MultiSet) Erase(value octosql.Value) error {
	set.internal.Lock()
	defer set.internal.Unlock()

	out := set.internal.tree.Get(&keyCount{
		key: value.MonotonicMarshal(),
	})
	if out == nil {
		out = &keyCount{
			key:   value.MonotonicMarshal(),
			count: 0,
		}
	}
	count := out.(*keyCount).count
	count--

	if count == 0 {
		set.internal.tree.Delete(&keyCount{
			key: value.MonotonicMarshal(),
		})
	} else {
		set.internal.tree.ReplaceOrInsert(&keyCount{
			key:   value.MonotonicMarshal(),
			count: count,
		})
	}

	return nil
}

func (set *MultiSet) Contains(value octosql.Value) (bool, error) {
	set.internal.Lock()
	defer set.internal.Unlock()

	return set.internal.tree.Has(&keyCount{
		key: value.MonotonicMarshal(),
	}), nil
}

func (set *MultiSet) Clear() error {
	set.internal.Lock()
	defer set.internal.Unlock()

	set.internal.tree = btree.New(2)
	return nil
}

func (set *MultiSet) ReadAll() ([]octosql.Value, error) {
	set.internal.Lock()
	defer set.internal.Unlock()

	var items []octosql.Value
	set.internal.tree.Ascend(func(item btree.Item) bool {
		var value octosql.Value
		value.MonotonicUnmarshal(item.(*keyCount).key) // TODO: Error handling

		items = append(items, value)
		return true
	})

	return items, nil
}

func (set *MultiSet) GetIterator() *MultiSetIterator {
	set.internal.Lock()
	defer set.internal.Unlock()

	var items []octosql.Value
	var counts []int
	set.internal.tree.Ascend(func(item btree.Item) bool {
		var value octosql.Value
		value.MonotonicUnmarshal(item.(*keyCount).key) // TODO: Error handling

		items = append(items, value)
		counts = append(counts, item.(*keyCount).count)
		return true
	})

	return NewBTreeSetIterator(items, counts)
}

type MultiSetIterator struct {
	items  []octosql.Value
	counts []int
	i      int
}

func NewBTreeSetIterator(items []octosql.Value, counts []int) *MultiSetIterator {
	return &MultiSetIterator{
		items:  items,
		counts: counts,
		i:      0,
	}
}

func (msi *MultiSetIterator) Next(value *octosql.Value) error {
	if msi.i == len(msi.items) {
		return ErrEndOfIterator
	}

	*value = msi.items[msi.i]
	msi.counts[msi.i]--
	if msi.counts[msi.i] == 0 {
		msi.i++
	}

	return nil
}

func (msi *MultiSetIterator) Close() error {
	return nil
}

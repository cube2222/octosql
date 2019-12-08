package execution

import (
	"github.com/cube2222/octosql"
	"github.com/mitchellh/hashstructure"
	"github.com/pkg/errors"
)

type HashMap struct {
	container map[uint64][]entry
}

func NewHashMap() *HashMap {
	return &HashMap{
		container: make(map[uint64][]entry),
	}
}

type entry struct {
	key   octosql.Value
	value interface{}
}

func (hm *HashMap) Set(key octosql.Value, value interface{}) error {
	hash, err := hashstructure.Hash(key, nil)
	if err != nil {
		return errors.Wrapf(err, "couldn't hash %+v", key)
	}

	list := hm.container[hash]
	for i := range list {
		if octosql.AreEqual(list[i].key, key) {
			list[i].value = value
			return nil
		}
	}
	hm.container[hash] = append(list, entry{
		key:   key,
		value: value,
	})

	return nil
}

func (hm *HashMap) Get(key octosql.Value) (interface{}, bool, error) {
	hash, err := hashstructure.Hash(key, nil)
	if err != nil {
		return nil, false, errors.Wrapf(err, "couldn't hash %+v", key)
	}

	list := hm.container[hash]
	for i := range list {
		if octosql.AreEqual(list[i].key, key) {
			return list[i].value, true, nil
		}
	}
	return nil, false, nil
}

func (hm *HashMap) GetIterator() *Iterator {
	hashes := make([]uint64, 0, len(hm.container))
	for k := range hm.container {
		hashes = append(hashes, k)
	}

	return &Iterator{
		hm:             hm,
		hashes:         hashes,
		hashesPosition: 0,
		listPosition:   0,
	}
}

type Iterator struct {
	hm             *HashMap
	hashes         []uint64
	hashesPosition int
	listPosition   int
}

// Next returns next key, value, exists
func (iter *Iterator) Next() (octosql.Value, interface{}, bool) {
	if iter.hashesPosition == len(iter.hashes) {
		return octosql.ZeroValue(), nil, false
	}

	// Save current item location
	outHashPos := iter.hashesPosition
	outListPos := iter.listPosition

	// Advance iterator to next item
	if iter.listPosition+1 == len(iter.hm.container[iter.hashes[iter.hashesPosition]]) {
		iter.hashesPosition++
		iter.listPosition = 0
	} else {
		iter.listPosition++
	}

	outEntry := iter.hm.container[iter.hashes[outHashPos]][outListPos]
	return outEntry.key, outEntry.value, true
}

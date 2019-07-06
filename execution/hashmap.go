package execution

import (
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
	key   interface{}
	value interface{}
}

func (g *HashMap) Set(key interface{}, value interface{}) error {
	hash, err := hashstructure.Hash(key, nil)
	if err != nil {
		return errors.Wrapf(err, "couldn't hash %+v", key)
	}

	list := g.container[hash]
	for i := range list {
		if AreEqual(list[i].key, key) {
			list[i].value = value
			return nil
		}
	}
	g.container[hash] = append(list, entry{
		key:   key,
		value: value,
	})

	return nil
}

func (g *HashMap) Get(key interface{}) (interface{}, bool, error) {
	hash, err := hashstructure.Hash(key, nil)
	if err != nil {
		return nil, false, errors.Wrapf(err, "couldn't hash %+v", key)
	}

	list := g.container[hash]
	for i := range list {
		if AreEqual(list[i].key, key) {
			return list[i].value, true, nil
		}
	}
	return nil, false, nil
}

func (g *HashMap) GetIterator() *Iterator {
	hashes := make([]uint64, 0, len(g.container))
	for k := range g.container {
		hashes = append(hashes, k)
	}

	return &Iterator{
		hm:             g,
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
func (iter *Iterator) Next() (interface{}, interface{}, bool) {
	if iter.hashesPosition == len(iter.hashes) {
		return nil, nil, false
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

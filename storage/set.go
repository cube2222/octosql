package storage

import (
	"github.com/cube2222/octosql"

	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/mitchellh/hashstructure"
	"github.com/pkg/errors"
)

type hashingFunction func(octosql.Value) ([]byte, error)

const valueNotPresent = -1

type MultiSet struct {
	tx StateTransaction
}

type MultiSetIterator struct {
	it               Iterator
	currentHashTuple []octosql.Value
	currentCounter   int
}

func NewMultiSet(tx StateTransaction) *MultiSet {
	return &MultiSet{
		tx: tx,
	}
}

func (set *MultiSet) GetCount(value octosql.Value) (int, error) {
	return set.getCountWithFunction(value, hashValue)
}

func (set *MultiSet) getCountWithFunction(value octosql.Value, hashFunction hashingFunction) (int, error) {
	hash, err := hashFunction(value)
	if err != nil {
		return 0, errors.Wrap(err, "couldn't hash value")
	}

	// Get state of given hash
	state := NewValueState(set.tx.WithPrefix(hash))

	var tuple octosql.Value
	err = state.Get(&tuple)

	if err == ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, errors.Wrap(err, "failed to read state for hash")
	}

	sliceOfValues := tuple.AsSlice()
	index := getPositionInTuple(sliceOfValues, value)

	if index == valueNotPresent {
		return 0, nil
	}

	return getCountFromPair(sliceOfValues[index]), nil
}

func (set *MultiSet) Insert(value octosql.Value) error {
	return set.insertWithFunction(value, hashValue)
}

func (set *MultiSet) insertWithFunction(value octosql.Value, hashFunction hashingFunction) error {
	hash, err := hashFunction(value)
	if err != nil {
		return errors.Wrap(err, "couldn't parse given value in insert")
	}

	// Get state of given hash
	state := NewValueState(set.tx.WithPrefix(hash))

	// Get elements with that hash
	var tuple octosql.Value
	err = state.Get(&tuple)

	if err == ErrNotFound {
		tuple = octosql.ZeroTuple()
	} else if err != nil {
		return errors.Wrap(err, "failed to read set elements")
	}

	tupleSlice := tuple.AsSlice()
	index := getPositionInTuple(tupleSlice, value)

	// If element isn't present we create a new pair with the count 1
	if index == valueNotPresent {
		tupleSlice = append(tupleSlice, makePair(value, 1))
	} else { // otherwise we increase the current count by 1
		currentCount := getCountFromPair(tupleSlice[index])
		tupleSlice[index] = makePair(value, currentCount+1)
	}

	// Store the new pairings
	newTuple := octosql.MakeTuple(tupleSlice)
	if err := state.Set(&newTuple); err != nil {
		return errors.Wrap(err, "couldn't store new tuple in hash state")
	}

	return nil
}

func (set *MultiSet) Erase(value octosql.Value) error {
	return set.eraseWithFunction(value, hashValue)
}

func (set *MultiSet) eraseWithFunction(value octosql.Value, hashFunction hashingFunction) error {
	hash, err := hashFunction(value)
	if err != nil {
		return errors.Wrap(err, "couldn't hash value")
	}

	hashedTxn := set.tx.WithPrefix(hash)
	state := NewValueState(hashedTxn)

	var tuple octosql.Value
	err = state.Get(&tuple)

	if err == ErrNotFound {
		return nil //the element wasn't present in the set
	} else if err != nil {
		return errors.Wrap(err, "failed to read set elements")
	}

	tupleSlice := tuple.AsSlice()
	index := getPositionInTuple(tupleSlice, value)

	if index == -1 {
		return nil // the element wasn't present
	}

	valueCount := getCountFromPair(tupleSlice[index])
	newValueCount := valueCount - 1

	var newTuple octosql.Value

	// If the element is still present after erasing one of it's copies, we just decrease the count
	if newValueCount > 0 {
		tupleSlice[index] = makePair(value, newValueCount)
		newTuple = octosql.MakeTuple(tupleSlice)
	} else { // otherwise we have to remove it from the slice
		newTuple = removeElementFromTuple(index, tupleSlice)
	}

	// If the removed value is last of its hash we clear it
	if len(newTuple.AsSlice()) == 0 {
		err := set.tx.Delete(hash)
		if err != nil {
			return errors.Wrap(err, "couldn't clear given hash after removing an element")
		}

		return nil
	}

	// Otherwise we store it
	err = state.Set(&newTuple)
	if err != nil {
		return errors.Wrap(err, "failed to actualize value of hash after removing an element")
	}

	return nil
}

// Returns the position of a value in a tuple if it's a member of the tuple,
// and -1 if it's not. Each value is in fact a tuple of (value, count)
func getPositionInTuple(values []octosql.Value, value octosql.Value) int {
	for i, pair := range values {
		if octosql.AreEqual(getValueFromPair(pair), value) {
			return i
		}
	}

	return valueNotPresent
}

// Removes the value at a specified index
func removeElementFromTuple(index int, values []octosql.Value) octosql.Value {
	length := len(values)
	result := values[:index]

	if index != length-1 {
		result = append(result, values[index+1:]...)
	}

	return octosql.MakeTuple(result)
}

func (set *MultiSet) Contains(value octosql.Value) (bool, error) {
	return set.containsWithFunction(value, hashValue)
}

func (set *MultiSet) containsWithFunction(value octosql.Value, hashFunction func(octosql.Value) ([]byte, error)) (bool, error) {
	count, err := set.getCountWithFunction(value, hashFunction)
	if err != nil {
		return false, errors.Wrap(err, "couldn't get values count")
	}

	return count > 0, nil
}

func (set *MultiSet) Clear() error {
	return set.clearUsingHash(hashValue)
}

func (set *MultiSet) clearUsingHash(hashFunction func(octosql.Value) ([]byte, error)) error {
	it := set.GetIterator()
	defer it.Close()

	var value octosql.Value

	err := it.Next(&value)

	for err != ErrEndOfIterator {
		hash, hashErr := hashFunction(value)

		if hashErr != nil {
			return errors.Wrap(hashErr, "failed to hash value")
		}

		deleteErr := set.tx.Delete(hash)
		if deleteErr != nil {
			return errors.Wrap(deleteErr, "failed to delete value")
		}

		err = it.Next(&value)
	}

	return nil
}

func (set *MultiSet) ReadAll() ([]octosql.Value, error) {
	it := set.GetIterator()

	values := make([]octosql.Value, 0)
	var value octosql.Value

	for {
		err := it.Next(&value)
		if err == ErrEndOfIterator {
			err := it.Close()
			if err != nil {
				return nil, errors.Wrap(err, "couldn't close iterator")
			}

			return values, nil
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't read next value from set")
		}

		values = append(values, value)
	}
}

func (set *MultiSet) GetIterator() *MultiSetIterator {
	it := set.tx.Iterator(WithDefault())
	return NewSetIterator(it)
}

func NewSetIterator(it Iterator) *MultiSetIterator {
	return &MultiSetIterator{
		it:               it,
		currentHashTuple: []octosql.Value{},
		currentCounter:   0,
	}
}

func (msi *MultiSetIterator) Next(value *octosql.Value) error {
	var tuple octosql.Value

	if msi.currentCounter == len(msi.currentHashTuple) {
		err := msi.it.Next(&tuple)
		if err == ErrEndOfIterator {
			return ErrEndOfIterator
		} else if err != nil {
			return errors.Wrap(err, "failed to read next tuple from underlying iterator")
		}

		msi.currentHashTuple = tuple.AsSlice()
		msi.currentCounter = 0
	}

	currentPair := msi.currentHashTuple[msi.currentCounter]
	*value = getValueFromPair(currentPair)
	newValue := getCountFromPair(currentPair) - 1

	if newValue == 0 {
		msi.currentCounter++
	} else {
		msi.currentHashTuple[msi.currentCounter] = makePair(*value, newValue)
	}

	return nil
}

func makePair(value octosql.Value, count int) octosql.Value {
	pair := make([]octosql.Value, 2)
	pair[0] = value
	pair[1] = octosql.MakeInt(count)

	return octosql.MakeTuple(pair)
}

func getValueFromPair(pair octosql.Value) octosql.Value {
	return pair.AsSlice()[0]
}

func getCountFromPair(pair octosql.Value) int {
	return pair.AsSlice()[1].AsInt()
}

func (msi *MultiSetIterator) Close() error {
	return msi.it.Close()
}

// The function used to hash values
func hashValue(value octosql.Value) ([]byte, error) {
	valueBytes, err := proto.Marshal(&value)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't marshal value")
	}

	hashValue, err := hashstructure.Hash(valueBytes, nil)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't hash value")
	}

	hashKey := make([]byte, 8)
	binary.BigEndian.PutUint64(hashKey, hashValue)

	return hashKey, nil
}

package execution

import (
	"sort"

	"github.com/cube2222/octosql"
	"github.com/mitchellh/hashstructure"
	"github.com/pkg/errors"
)

type multiSetElement struct {
	rec   *Record
	count int
}

type recordMultiSet struct {
	set map[uint64][]multiSetElement
}

func newMultiSetElement(rec *Record) multiSetElement {
	return multiSetElement{
		rec:   rec,
		count: 1,
	}
}

func newMultiSet() *recordMultiSet {
	return &recordMultiSet{
		set: make(map[uint64][]multiSetElement),
	}
}

func (rms *recordMultiSet) Insert(rec *Record) error {
	hash, err := hashstructure.Hash(rec, nil)
	if err != nil {
		return errors.Wrap(err, "couldn't hash record")
	}

	targetSlice := rms.set[hash]
	for k := range targetSlice {
		element := targetSlice[k]
		if AreEqual(element.rec, rec) {
			element.count++
			return nil
		}
	}

	rms.set[hash] = append(rms.set[hash], newMultiSetElement(rec))

	return nil
}

func (rms *recordMultiSet) GetCount(rec *Record) (int, error) {
	hash, err := hashstructure.Hash(rec, nil)
	if err != nil {
		return 0, errors.Wrap(err, "couldn't hash record")
	}

	targetSlice := rms.set[hash]
	for k := range targetSlice {
		element := targetSlice[k]
		if AreEqual(element.rec, rec) {
			return element.count, nil
		}
	}

	return 0, nil
}

type entity struct {
	fieldName octosql.VariableName
	value     interface{}
}

type row []entity

func newEntity(name octosql.VariableName, value interface{}) entity {
	return entity{
		fieldName: name,
		value:     value,
	}
}

func Normalize(rec *Record) *Record {
	row := make(row, 0)
	for k := range rec.fieldNames {
		fieldName := rec.fieldNames[k]
		value := rec.data[k]
		row = append(row, newEntity(fieldName, value))
	}

	sort.Slice(row, func(i, j int) bool {
		return row[i].fieldName < row[j].fieldName
	})

	sortedFieldNames := make([]octosql.VariableName, len(rec.fieldNames))
	values := make([]interface{}, len(rec.fieldNames))

	for k := range row {
		ent := row[k]
		sortedFieldNames[k] = ent.fieldName
		values[k] = ent.value
	}

	return UtilNewRecord(sortedFieldNames, values)
}

func AreStreamsEqual(first, second RecordStream) (bool, error) {
	firstMultiSet := newMultiSet()
	secondMultiSet := newMultiSet()

	for {
		firstRec, firstErr := first.Next()
		secondRec, secondErr := second.Next()

		if firstErr == secondErr && firstErr == ErrEndOfStream {
			break
		} else if firstErr == ErrEndOfStream && secondErr == nil {
			return false, nil
		} else if firstErr == nil && secondErr == ErrEndOfStream {
			return false, nil
		} else if firstErr != nil {
			return false, errors.Wrap(firstErr, "error in Next for first stream")
		} else if secondErr != nil {
			return false, errors.Wrap(secondErr, "error in Next for second stream")
		}

		err := firstMultiSet.Insert(Normalize(firstRec))
		if err != nil {
			return false, errors.Wrap(err, "couldn't insert into the multiset")
		}

		err = secondMultiSet.Insert(Normalize(secondRec))
		if err != nil {
			return false, errors.Wrap(err, "couldn't insert into the multiset")
		}
	}

	firstContained, err := firstMultiSet.isContained(secondMultiSet)
	if err != nil {
		return false, errors.Wrap(err, "couldn't check whether first contained in second")
	}

	secondContained, err := secondMultiSet.isContained(firstMultiSet)
	if err != nil {
		return false, errors.Wrap(err, "couldn't check whether second contained in first")
	}

	if !(firstContained && secondContained) {
		return false, nil
	}

	return true, nil
}

func (rms *recordMultiSet) isContained(other *recordMultiSet) (bool, error) {
	for key := range rms.set {
		hashSlice := rms.set[key]
		for k := range hashSlice {
			setElem := hashSlice[k]

			otherCount, err := other.GetCount(setElem.rec)
			if err != nil {
				return false, errors.Wrap(err, "couldn't get count of elem in second set")
			}

			if otherCount < setElem.count {
				return false, nil
			}
		}
	}

	return true, nil
}

func UtilNewRecord(fields []octosql.VariableName, data []interface{}) *Record {
	return &Record{
		fieldNames: fields,
		data:       data,
	}
}

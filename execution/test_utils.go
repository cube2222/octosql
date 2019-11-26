package execution

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
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
	hash, err := HashRecord(rec)
	if err != nil {
		return errors.Wrap(err, "couldn't hash record")
	}

	targetSlice := rms.set[hash]
	for k := range targetSlice {
		element := targetSlice[k]
		if element.rec.Equal(rec) {
			rms.set[hash][k].count++
			return nil
		}
	}

	rms.set[hash] = append(rms.set[hash], newMultiSetElement(rec))

	return nil
}

func (rms *recordMultiSet) GetCount(rec *Record) (int, error) {
	hash, err := HashRecord(rec)
	if err != nil {
		return 0, errors.Wrap(err, "couldn't hash record")
	}

	targetSlice := rms.set[hash]
	for k := range targetSlice {
		element := targetSlice[k]
		if element.rec.Equal(rec) {
			return element.count, nil
		}
	}

	return 0, nil
}

type entity struct {
	fieldName octosql.VariableName
	value     octosql.Value
}

type row []entity

func newEntity(name octosql.VariableName, value octosql.Value) entity {
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

	return NewRecordFromSliceWithNormalize(sortedFieldNames, values)
}

func AreStreamsEqual(ctx context.Context, first, second RecordStream) (bool, error) {
	for {
		firstRec, firstErr := first.Next(ctx)
		secondRec, secondErr := second.Next(ctx)

		if firstErr == secondErr && firstErr == ErrEndOfStream {
			break
		} else if firstErr == ErrEndOfStream && secondErr == nil {
			return false, fmt.Errorf("no record in first stream, %s in second", secondRec.String())
		} else if firstErr == nil && secondErr == ErrEndOfStream {
			return false, fmt.Errorf("no record in second stream, %s in first", firstRec.String())
		} else if firstErr != nil {
			return false, errors.Wrap(firstErr, "error in Next for first stream")
		} else if secondErr != nil {
			return false, errors.Wrap(secondErr, "error in Next for second stream")
		}

		if !firstRec.Equal(secondRec) {
			return false, fmt.Errorf("records not equal: %s and %s", firstRec.String(), secondRec.String())
		}
	}

	return true, nil
}

func AreStreamsEqualNoOrdering(ctx context.Context, first, second RecordStream) (bool, error) {
	firstMultiSet := newMultiSet()
	secondMultiSet := newMultiSet()

	for {
		firstRec, firstErr := first.Next(ctx)
		secondRec, secondErr := second.Next(ctx)

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

		err := firstMultiSet.Insert(firstRec)
		if err != nil {
			return false, errors.Wrap(err, "couldn't insert into the multiset")
		}

		err = secondMultiSet.Insert(secondRec)
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

func NewRecordFromSliceWithNormalize(fields []octosql.VariableName, data []interface{}, opts ...RecordOption) *Record {
	normalized := make([]octosql.Value, len(data))
	for i := range data {
		normalized[i] = octosql.NormalizeType(data[i])
	}
	return NewRecordFromSlice(fields, normalized, opts...)
}

func NewDummyNode(data []*Record) *DummyNode {
	return &DummyNode{
		data,
	}
}

type DummyNode struct {
	data []*Record
}

func (dn *DummyNode) Get(ctx context.Context, variables octosql.Variables) (RecordStream, error) {
	if dn.data == nil {
		return NewInMemoryStream([]*Record{}), nil
	}

	return NewInMemoryStream(dn.data), nil
}

func NewDummyValue(value octosql.Value) *DummyValue {
	return &DummyValue{
		value,
	}
}

type DummyValue struct {
	value octosql.Value
}

func (dv *DummyValue) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	return dv.value, nil
}

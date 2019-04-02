package execution

import (
	"sort"

	"github.com/pkg/errors"
)

type entity struct {
	fieldName string
	value     interface{}
}

//creates a entity slice from a record, sorts it by column name
//and normalizes types
func normalizeColumns(rec *Record) []entity {
	merged := make([]entity, 0)
	for k := range rec.fieldNames {
		newEntity := entity{
			fieldName: rec.fieldNames[k].String(),
			value:     NormalizeType(rec.data[k]),
		}
		merged = append(merged, newEntity)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].fieldName < merged[j].fieldName
	})

	return merged
}

//entities are considered equal iff after sorting len(first) == len(second)
//and for every 0 <= i < len(first) first[i].value == second[i].value
//								and first[i].fieldName == second[i].fieldName
func areEntitiesEqual(first, second []entity) bool {
	if len(first) != len(second) {
		return false
	}

	for k := range first {
		firstVal := first[k].value
		secondVal := second[k].value

		if !AreEqual(firstVal, secondVal) || first[k].fieldName != second[k].fieldName {
			return false
		}
	}
	return true
}

func AreStreamsEqual(first, second RecordStream) (bool, error) {
	firstTable := make([][]entity, 0)
	secondTable := make([][]entity, 0)

	for {
		firstRec, firstErr := first.Next()
		secondRec, secondErr := second.Next()
		if firstErr == ErrEndOfStream && secondErr == ErrEndOfStream {
			break
		} else if firstErr != nil {
			return false, errors.Wrap(firstErr, "couldn't get record")
		} else if secondErr != nil {
			return false, errors.Wrap(secondErr, "couldn't get record")
		}

		firstTable = append(firstTable, normalizeColumns(firstRec))
		secondTable = append(secondTable, normalizeColumns(secondRec))
	}

	sort.Slice(firstTable, func(i, j int) bool {
		return lessEntity(firstTable[i], firstTable[j])
	})

	sort.Slice(secondTable, func(i, j int) bool {
		return lessEntity(secondTable[i], secondTable[j])
	})

	for k := range firstTable {
		if !areEntitiesEqual(firstTable[k], secondTable[k]) {
			return false, nil
		}
	}

	return true, nil
}

func lessEntity(first, second []entity) bool {
	if len(first) != len(second) {
		return len(first) < len(second)
	}

	for k := range first {
		f := first[k].value
		s := second[k].value

		if !AreEqual(f, s) {
			return lessInterface(f, s)
		}
	}

	return false
}

func lessInterface(a, b interface{}) bool {
	switch a := a.(type) {
	case int:
		b, ok := b.(int)
		if !ok {
			return false
		}

		return a < b
	case string:
		b, ok := b.(string)
		if !ok {
			return false
		}

		return a < b
	case float64:
		b, ok := b.(float64)
		if !ok {
			return false
		}

		return a < b
	case bool:
		b, ok := b.(bool)
		if !ok {
			return false
		}

		return !a && b
	default:
		return false
	}
}

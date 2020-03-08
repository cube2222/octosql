package execution

import (
	"context"
	"fmt"
	"log"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type recordMultiSet struct {
	set   []*Record
	count []int
}

func newMultiSet() *recordMultiSet {
	return &recordMultiSet{}
}

func (rms *recordMultiSet) Insert(rec *Record) {
	for i := range rms.set {
		if rms.set[i].Equal(rec) {
			rms.count[i]++
			return
		}
	}

	rms.set = append(rms.set, rec)
	rms.count = append(rms.count, 1)
}

func (rms *recordMultiSet) GetCount(rec *Record) int {
	for i := range rms.set {
		if rms.set[i].Equal(rec) {
			return rms.count[i]
		}
	}

	return 0
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
	for k, fieldName := range rec.GetVariableNames() {
		value := *rec.Data[k]
		row = append(row, newEntity(fieldName, value))
	}

	sort.Slice(row, func(i, j int) bool {
		return row[i].fieldName < row[j].fieldName
	})

	fieldLength := len(rec.FieldNames)
	sortedFieldNames := make([]octosql.VariableName, fieldLength)
	values := make([]interface{}, fieldLength)

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

func AreStreamsEqualNoOrdering(ctx context.Context, stateStorage storage.Storage, first, second RecordStream) (bool, error) {
	firstMultiSet := newMultiSet()
	secondMultiSet := newMultiSet()

	firstRecords, err := ReadAll(ctx, stateStorage, first)
	if err != nil {
		return false, errors.Wrap(err, "couldn't read first stream records")
	}
	for _, rec := range firstRecords {
		firstMultiSet.Insert(rec)
	}

	secondRecords, err := ReadAll(ctx, stateStorage, second)
	if err != nil {
		return false, errors.Wrap(err, "couldn't read second stream records")
	}
	for _, rec := range secondRecords {
		secondMultiSet.Insert(rec)
	}

	firstContained := firstMultiSet.isContained(secondMultiSet)
	secondContained := secondMultiSet.isContained(firstMultiSet)
	if !(firstContained && secondContained) {
		return false, nil
	}

	return true, nil
}

func (rms *recordMultiSet) isContained(other *recordMultiSet) bool {
	for i, rec := range rms.set {
		myCount := rms.count[i]
		otherCount := other.GetCount(rec)
		if otherCount != myCount {
			return false
		}
	}

	return true
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

func (dn *DummyNode) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	if dn.data == nil {
		return NewInMemoryStream([]*Record{}), NewExecutionOutput(NewZeroWatermarkGenerator()), nil
	}

	return NewInMemoryStream(dn.data), NewExecutionOutput(NewZeroWatermarkGenerator()), nil
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

func ReadAll(ctx context.Context, stateStorage storage.Storage, stream RecordStream) ([]*Record, error) {
	var records []*Record
	for {
		tx := stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		rec, err := stream.Next(ctx)
		if err == ErrEndOfStream {
			err := tx.Commit()
			if err != nil {
				return nil, errors.Wrap(err, "couldn't commit transaction")
			}
			break
		} else if errors.Cause(err) == ErrNewTransactionRequired {
			err := tx.Commit()
			if err != nil {
				continue
			}
			continue
		} else if waitableError := GetErrWaitForChanges(err); waitableError != nil {
			err := tx.Commit()
			if err != nil {
				continue
			}
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("couldn't close subscription: ", err)
			}
			continue
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't get next record")
		}

		err = tx.Commit()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't commit transaction")
		}

		records = append(records, rec)
	}

	return records, nil
}

func GetTestStorage(t *testing.T) storage.Storage {
	opts := badger.DefaultOptions("")
	opts.Dir = ""
	opts.ValueDir = ""
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal("couldn't open in-memory badger database: ", err)
	}
	return storage.NewBadgerStorage(db)
}

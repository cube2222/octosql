package execution

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type recordMultiSet struct {
	set          []*Record
	count        []int
	equalityFunc RecordEqualityFunc
}

func newMultiSet(equalityFunc RecordEqualityFunc) *recordMultiSet {
	return &recordMultiSet{
		equalityFunc: equalityFunc,
	}
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
	count := 0
	for i := range rms.set {
		if rms.equalityFunc(rms.set[i], rec) == nil {
			count += rms.count[i]
		}
	}

	return count
}

func (rms *recordMultiSet) Erase(rec *Record) {
	for i := range rms.set {
		if rms.set[i].Equal(rec) {
			rms.count[i]--
			return
		}
	}

	rms.set = append(rms.set, rec)
	rms.count = append(rms.count, -1)
}

func (rms *recordMultiSet) isContainedIn(other *recordMultiSet) bool {
	for _, rec := range rms.set {
		myCount := rms.GetCount(rec)
		otherCount := other.GetCount(rec)
		if myCount > otherCount {
			return false
		}
	}

	return true
}

func (rms *recordMultiSet) Show() string {
	recordStrings := make([]string, len(rms.set))
	for i := range rms.set {
		recordStrings[i] = fmt.Sprintf("%s: %d", rms.set[i].Show(), rms.count[i])
	}

	return fmt.Sprintf("{%s}", strings.Join(recordStrings, "\n"))
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

func AreStreamsEqual(ctx context.Context, first, second RecordStream) error {
	for {
		firstRec, firstErr := first.Next(ctx)
		secondRec, secondErr := second.Next(ctx)

		if firstErr == secondErr && firstErr == ErrEndOfStream {
			break
		} else if firstErr == ErrEndOfStream && secondErr == nil {
			return fmt.Errorf("no record in first stream, %s in second", secondRec.String())
		} else if firstErr == nil && secondErr == ErrEndOfStream {
			return fmt.Errorf("no record in second stream, %s in first", firstRec.String())
		} else if firstErr != nil {
			return errors.Wrap(firstErr, "error in Next for first stream")
		} else if secondErr != nil {
			return errors.Wrap(secondErr, "error in Next for second stream")
		}

		if !firstRec.Equal(secondRec) {
			return fmt.Errorf("records not equal: %s and %s", firstRec.String(), secondRec.String())
		}
	}

	return nil
}

type AreEqualConfig struct {
	Equality RecordEqualityFunc
}

type RecordEqualityFunc func(record1 *Record, record2 *Record) error

type AreEqualOpt func(*AreEqualConfig)

func WithEqualityBasedOn(fs ...RecordEqualityFunc) AreEqualOpt {
	return func(config *AreEqualConfig) {
		config.Equality = EqualityOfAll(fs...)
	}
}

func EqualityOfAll(fs ...RecordEqualityFunc) func(record1 *Record, record2 *Record) error {
	return func(record1 *Record, record2 *Record) error {
		for _, f := range fs {
			if err := f(record1, record2); err != nil {
				return err
			}
		}
		return nil
	}
}

func EqualityOfID(record1 *Record, record2 *Record) error {
	if record1.ID() != record2.ID() {
		return errors.Errorf("ID's not equal: %s and %s", record1.ID().Show(), record2.ID().Show())
	}
	return nil
}

func EqualityOfUndo(record1 *Record, record2 *Record) error {
	if record1.IsUndo() != record2.IsUndo() {
		return errors.Errorf("undo's not equal: %t and %t", record1.IsUndo(), record2.IsUndo())
	}
	return nil
}

func EqualityOfEventTimeField(record1 *Record, record2 *Record) error {
	if record1.EventTimeField() != record2.EventTimeField() {
		return errors.Errorf("event time fields not equal: %s and %s", record1.EventTimeField(), record2.EventTimeField())
	}
	return nil
}

func EqualityOfFieldsAndValues(record1 *Record, record2 *Record) error {
	if len(record1.Fields()) != len(record2.Fields()) {
		return errors.Errorf("field counts not equal: %d and %d", len(record1.Fields()), len(record2.Fields()))
	}

	fields1 := record1.Fields()
	fields2 := record2.Fields()
	for i := range fields1 {
		if !fields1[i].Name.Equal(fields2[i].Name) {
			return errors.Errorf("field at index %d not equal: %s and %s", i, fields1[i].Name.String(), fields2[i].Name.String())
		}
	}
	for i := range fields1 {
		if !octosql.AreEqual(record1.Value(fields1[i].Name), record2.Value(fields1[i].Name)) {
			return errors.Errorf("value with field name %s: %s and %s", fields1[i].Name.String(), record1.Value(fields1[i].Name).Show(), record2.Value(fields1[i].Name).Show())
		}
	}
	return nil
}

func EqualityOfEverythingButIDs(record1 *Record, record2 *Record) error {
	return EqualityOfAll(EqualityOfFieldsAndValues, EqualityOfEventTimeField, EqualityOfUndo)(record1, record2)
}

func DefaultEquality(record1 *Record, record2 *Record) error {
	if !record1.Equal(record2) {
		return errors.Errorf("records not equal in terms of default equality")
	}
	return nil
}

func AreStreamsEqualNoOrderingWithRetractionReductionAndIDChecking(ctx context.Context, stateStorage storage.Storage, got, want RecordStream, opts ...AreEqualOpt) error {
	config := &AreEqualConfig{
		Equality: DefaultEquality,
	}

	for _, opt := range opts {
		opt(config)
	}

	firstMultiSet := newMultiSet(config.Equality)
	secondMultiSet := newMultiSet(config.Equality)

	log.Println("got stream")
	gotRecords, err := ReadAll(ctx, stateStorage, got)
	if err != nil {
		return errors.Wrap(err, "couldn't read got streams records")
	}

	if err := areIDsUniqueAndFromSameStream(gotRecords); err != nil {
		return errors.Wrap(err, "record ids aren't unique, or don't come from the same stream")
	}

	for _, rec := range gotRecords {
		// Store record with respect to whether it's a retraction or not

		if rec.IsUndo() {
			firstMultiSet.Erase(rec)
		} else {
			firstMultiSet.Insert(rec)
		}
	}

	log.Println("want stream")
	wantRecords, err := ReadAll(ctx, stateStorage, want)
	if err != nil {
		return errors.Wrap(err, "couldn't read want streams records")
	}

	for _, rec := range wantRecords {
		if rec.IsUndo() {
			secondMultiSet.Erase(rec)
		} else {
			secondMultiSet.Insert(rec)
		}
	}

	firstContained := firstMultiSet.isContainedIn(secondMultiSet)
	secondContained := secondMultiSet.isContainedIn(firstMultiSet)
	if !(firstContained && secondContained) {
		return errors.Errorf("different sets: \n %s \n and \n %s", firstMultiSet.Show(), secondMultiSet.Show())
	}

	return nil
}

func AreStreamsEqualNoOrderingWithIDCheck(ctx context.Context, stateStorage storage.Storage, gotStream, wantStream RecordStream, opts ...AreEqualOpt) error {
	config := &AreEqualConfig{
		Equality: DefaultEquality,
	}
	for _, opt := range opts {
		opt(config)
	}

	firstMultiSet := newMultiSet(config.Equality)
	secondMultiSet := newMultiSet(config.Equality)

	log.Println("first stream")
	gotRecords, err := ReadAll(ctx, stateStorage, gotStream)
	if err != nil {
		return errors.Wrap(err, "couldn't read first streams records")
	}
	for _, rec := range gotRecords {
		firstMultiSet.Insert(rec)
	}
	log.Println("read first stream")

	log.Println("second stream")
	wantRecords, err := ReadAll(ctx, stateStorage, wantStream)
	if err != nil {
		return errors.Wrap(err, "couldn't read second streams records")
	}
	for _, rec := range wantRecords {
		secondMultiSet.Insert(rec)
	}
	log.Println("read second stream")

	firstContained := firstMultiSet.isContainedIn(secondMultiSet)
	secondContained := secondMultiSet.isContainedIn(firstMultiSet)
	if !(firstContained && secondContained) {
		return errors.Errorf("different sets: \n %s \n and \n %s", firstMultiSet.Show(), secondMultiSet.Show())
	}

	if err := areIDsUniqueAndFromSameStream(gotRecords); err != nil {
		return errors.Wrap(err, "record ids aren't unique, or don't come from the same stream")
	}

	return nil
}

func areIDsUniqueAndFromSameStream(records []*Record) error {
	if len(records) == 0 {
		return nil
	}

	seenIDs := make(map[int64]struct{})
	var wantID string

	// All recordIDs should be of form X.index, where X is the same streamID for every record and indexes are distinct
	for i, rec := range records {
		recordIDBase, offsetAsInt, err := splitID(rec.ID())
		if err != nil {
			return errors.Wrapf(err, "couldn't parse record ID %v", rec.ID().ID)
		}

		if i == 0 {
			wantID = recordIDBase
		} else {
			if wantID != recordIDBase {
				return errors.Errorf("got a record with base %v but expected it to be %v", recordIDBase, wantID)
			}
		}

		_, ok := seenIDs[offsetAsInt]
		if ok {
			return errors.Errorf("ids with number %v were repeated", offsetAsInt)
		}
		seenIDs[offsetAsInt] = struct{}{}
	}

	return nil
}

func splitID(ID *RecordID) (string, int64, error) {
	splitByComa := strings.Split(ID.ID, ".")
	if len(splitByComa) != 2 {
		return "", 0, errors.Errorf("got a record ID that didn't split into two parts when split by a coma: %v", ID.ID)
	}

	numberAsInt, err := strconv.ParseInt(splitByComa[1], 10, 64)
	if err != nil {
		return "", 0, errors.Wrap(err, "couldn't parse second part of record ID as number")
	}

	return splitByComa[0], numberAsInt, nil
}

func AreStreamsEqualNoOrdering(ctx context.Context, stateStorage storage.Storage, first, second RecordStream, opts ...AreEqualOpt) error {
	config := &AreEqualConfig{
		Equality: DefaultEquality,
	}
	for _, opt := range opts {
		opt(config)
	}

	firstMultiSet := newMultiSet(config.Equality)
	secondMultiSet := newMultiSet(config.Equality)

	log.Println("first stream")
	firstRecords, err := ReadAll(ctx, stateStorage, first)
	if err != nil {
		return errors.Wrap(err, "couldn't read first streams records")
	}
	for _, rec := range firstRecords {
		firstMultiSet.Insert(rec)
	}
	log.Println("read first stream")

	log.Println("second stream")
	secondRecords, err := ReadAll(ctx, stateStorage, second)
	if err != nil {
		return errors.Wrap(err, "couldn't read second streams records")
	}
	for _, rec := range secondRecords {
		secondMultiSet.Insert(rec)
	}
	log.Println("read second stream")

	firstContained := firstMultiSet.isContainedIn(secondMultiSet)
	secondContained := secondMultiSet.isContainedIn(firstMultiSet)
	if !(firstContained && secondContained) {
		return errors.Errorf("different sets: \n %s \n and \n %s", firstMultiSet.Show(), secondMultiSet.Show())
	}

	return nil
}

func AreStreamsEqualWithOrdering(ctx context.Context, stateStorage storage.Storage, first, second RecordStream) error {

	log.Println("first stream")
	firstRecords, err := ReadAll(ctx, stateStorage, first)
	if err != nil {
		return errors.Wrap(err, "couldn't read first streams records")
	}
	log.Println("read first stream")

	log.Println("second stream")
	secondRecords, err := ReadAll(ctx, stateStorage, second)
	if err != nil {
		return errors.Wrap(err, "couldn't read second streams records")
	}
	log.Println("read second stream")

	if len(firstRecords) != len(secondRecords) {
		return errors.Errorf("streams have different amount of records")
	}

	for i := range firstRecords {
		if !firstRecords[i].Equal(secondRecords[i]) {
			return fmt.Errorf("records not equal: %s and %s", firstRecords[i].String(), secondRecords[i].String())
		}
	}

	return nil
}

func AreStreamsEqualNoOrderingWithCount(ctx context.Context, stateStorage storage.Storage, first, second RecordStream, count int) error {
	firstMultiSet := newMultiSet(DefaultEquality)
	secondMultiSet := newMultiSet(DefaultEquality)

	firstRecords, err := ReadAllWithCount(ctx, stateStorage, first, count)
	if err != nil {
		return errors.Wrap(err, "couldn't read first stream records")
	}
	for _, rec := range firstRecords {
		firstMultiSet.Insert(rec)
	}

	secondRecords, err := ReadAllWithCount(ctx, stateStorage, second, count)
	if err != nil {
		return errors.Wrap(err, "couldn't read second stream records")
	}
	for _, rec := range secondRecords {
		secondMultiSet.Insert(rec)
	}

	firstContained := firstMultiSet.isContainedIn(secondMultiSet)
	secondContained := secondMultiSet.isContainedIn(firstMultiSet)
	if !(firstContained && secondContained) {
		return errors.Errorf("different sets: %s and %s", firstMultiSet.Show(), secondMultiSet.Show())
	}

	return nil
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
		return NewInMemoryStream(ctx, []*Record{}), NewExecutionOutput(NewZeroWatermarkGenerator(), map[string]ShuffleData{}, nil), nil
	}

	return NewInMemoryStream(ctx, dn.data), NewExecutionOutput(NewZeroWatermarkGenerator(), map[string]ShuffleData{}, nil), nil
}

type ConstantValue struct {
	value octosql.Value
}

func NewConstantValue(value octosql.Value) *ConstantValue {
	return &ConstantValue{
		value,
	}
}

func (dv *ConstantValue) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	return dv.value, nil
}

func ReadAll(ctx context.Context, stateStorage storage.Storage, stream RecordStream) ([]*Record, error) {
	var records []*Record
	for {
		tx := stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		rec, err := stream.Next(ctx)
		if rec != nil {
			log.Println(rec.Show())
		} else {
			log.Println("no record: ", err)
		}
		if err == ErrEndOfStream {
			err := tx.Commit()
			if err != nil {
				continue
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
			continue
		}

		records = append(records, rec)
	}

	return records, nil
}

func ReadAllWithCount(ctx context.Context, stateStorage storage.Storage, stream RecordStream, count int) ([]*Record, error) {
	var records []*Record
	for i := 0; i < count; {
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
			_ = tx.Commit()
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

		i++
		records = append(records, rec)
	}

	return records, nil
}

type GetTestStreamOption func(*StreamID)

func GetTestStreamWithStreamID(id *StreamID) GetTestStreamOption {
	return func(old *StreamID) {
		*old = *id
	}
}

func GetTestStream(t *testing.T, stateStorage storage.Storage, variables octosql.Variables, node Node, opts ...GetTestStreamOption) RecordStream {
	streamID := GetRawStreamID()
	for _, opt := range opts {
		opt(streamID)
	}

	tx := stateStorage.BeginTransaction()
	stream, execOutput, err := node.Get(storage.InjectStateTransaction(context.Background(), tx), variables, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for _, task := range execOutput.TasksToRun {
		go task()
	}
	return stream
}

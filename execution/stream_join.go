package execution

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type JoinType int

const (
	INNER_JOIN JoinType = 0
	LEFT_JOIN  JoinType = 1
	OUTER_JOIN JoinType = 2
)

func (j JoinType) String() string {
	switch j {
	case INNER_JOIN:
		return "Inner Join"
	case LEFT_JOIN:
		return "Left Join"
	case OUTER_JOIN:
		return "Outer Join"
	default:
		return "Unknown Join"
	}
}

type StreamJoin struct {
	leftSource, rightSource Node
	leftKey, rightKey       []Expression
	storage                 storage.Storage
	eventTimeField          octosql.VariableName
	joinType                JoinType
	triggerPrototype        TriggerPrototype
}

func NewStreamJoin(leftSource, rightSource Node, leftKey, rightKey []Expression, storage storage.Storage, eventTimeField octosql.VariableName, joinType JoinType, triggerPrototype TriggerPrototype) *StreamJoin {
	return &StreamJoin{
		leftKey:          leftKey,
		rightKey:         rightKey,
		leftSource:       leftSource,
		rightSource:      rightSource,
		storage:          storage,
		eventTimeField:   eventTimeField,
		joinType:         joinType,
		triggerPrototype: triggerPrototype,
	}
}

func (node *StreamJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)

	// Get IDs of streams
	leftSourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakeString("left"))
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get left source stream ID")
	}

	rightSourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakeString("right"))
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get right source stream ID")
	}

	leftStream, leftExec, err := node.leftSource.Get(ctx, variables, leftSourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get left source stream in stream join")
	}

	rightStream, rightExec, err := node.rightSource.Get(ctx, variables, rightSourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get right source stream in stream join")
	}

	// The source of watermarks for a stream join is the minimum of watermarks from its sources
	watermarkSource := NewUnionWatermarkGenerator([]WatermarkSource{leftExec.WatermarkSource, rightExec.WatermarkSource})

	trigger, err := node.triggerPrototype.Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get trigger from trigger prototype")
	}

	// Create the shuffles
	mergedNextShuffles, err := mergeNextShuffles(leftExec.NextShuffles, rightExec.NextShuffles)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge next shuffles of sources")
	}

	stream := &JoinedStream{
		streamID:       streamID,
		eventTimeField: node.eventTimeField,
		joinType:       node.joinType,
	}

	processFunc := &ProcessByKey{
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpressions:  [][]Expression{node.leftKey, node.rightKey},
		processFunction: stream,
		variables:       variables,
	}

	streamJoinPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{leftStream, rightStream}, streamID, watermarkSource, true, ctx)

	return streamJoinPullEngine,
		NewExecutionOutput(
			streamJoinPullEngine,
			mergedNextShuffles,
			append(leftExec.TasksToRun, append(rightExec.TasksToRun, func() error { streamJoinPullEngine.Run(); return nil })...),
		), nil
}

type JoinedStream struct {
	streamID       *StreamID
	eventTimeField octosql.VariableName
	joinType       JoinType
}

var leftStreamOldRecordsPrefix = []byte("$left_stream_old_records$")
var rightStreamOldRecordsPrefix = []byte("$right_stream_old_records$")
var leftStreamNewRecordsPrefix = []byte("$left_stream_new_records$")
var rightStreamNewRecordsPrefix = []byte("$right_stream_new_records$")

func (js *JoinedStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex < 0 || inputIndex > 1 {
		panic("invalid inputIndex for stream join")
	}

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	isLeft := inputIndex == 0       // from which input source the record was received
	isRetraction := record.IsUndo() // is the incoming record a retraction

	// We get the key of the new incoming record
	WithNoUndo()(record) // reset the potential retraction information, since we want a record and its retraction "to find each other"
	newRecordValue := recordToValue(record)
	newRecordKey, err := proto.Marshal(&newRecordValue)
	if err != nil {
		return errors.Wrap(err, "couldn't marshall new record")
	}

	var myRecordSet *storage.MultiSet

	// Important note:
	// 1) If we are adding the record it's natural that we want to match it with every record from the other stream
	// 2) If we are retracting, we want to also retract every matching between the new record and records from the other stream.
	// 	  This is because if records arrive in order R1, L1, L2, R2, R3 and then we retract L1, it has matched
	//	  with R1 (when L1 was added) and with R2 and R3 (when they were added)
	if isLeft {
		myRecordSet = storage.NewMultiSet(txByKey.WithPrefix(leftStreamNewRecordsPrefix))
	} else {
		myRecordSet = storage.NewMultiSet(txByKey.WithPrefix(rightStreamNewRecordsPrefix))
	}

	// Keep track of the count of the record (fields + data, no metadata)
	newCount, err := keepTrackOfCount(txByKey.WithPrefix(newRecordKey), isRetraction)
	if err != nil {
		return err
	}

	// If we got an early retraction, or a record bumped up the count to 0, but it was previously retracted we do nothing
	if newCount < 0 || (newCount == 0 && !isRetraction) {
		return nil
	}

	// If the record is a retraction there are two options:
	// 1) It's still present in the set, then we just remove it
	// 2) It's not present, so it has already been triggered, so we send a retraction for it
	if isRetraction {
		present, err := myRecordSet.Contains(newRecordValue)
		if err != nil {
			return errors.Wrap(err, "couldn't check if record is present in the appropriate record set")
		}

		if present {
			// Present, so remove it
			if err := myRecordSet.Erase(newRecordValue); err != nil {
				return errors.Wrap(err, "couldn't erase record from the appropriate record set")
			}
		} else {
			// Otherwise send a retraction
			retractionRecord := NewRecordFromRecord(record, WithUndo())
			if err := myRecordSet.Insert(recordToValue(retractionRecord)); err != nil {
				return errors.Wrap(err, "couldn't insert retraction for record into the appropriate record set")
			}
		}
	} else {
		// Just insert it into the set
		if err := myRecordSet.Insert(newRecordValue); err != nil {
			return errors.Wrap(err, "couldn't insert new record into the appropriate record set")
		}
	}

	return nil
}

func keepTrackOfCount(tx storage.StateTransaction, isRetraction bool) (int, error) {
	var recordCount octosql.Value
	recordCountState := storage.NewValueState(tx)

	err := recordCountState.Get(&recordCount)
	if err == storage.ErrNotFound {
		recordCount = octosql.MakeInt(0)
	} else if err != nil {
		return 0, errors.Wrap(err, "couldn't check the count of record")
	}

	var updatedRecordCount octosql.Value
	if isRetraction {
		updatedRecordCount = octosql.MakeInt(recordCount.AsInt() - 1)
	} else {
		updatedRecordCount = octosql.MakeInt(recordCount.AsInt() + 1)
	}

	// Store the new count
	if updatedRecordCount.AsInt() == 0 {
		err = recordCountState.Clear()
		if err != nil {
			return 0, errors.Wrap(err, "couldn't clear count state of record")
		}
	} else {
		err = recordCountState.Set(&updatedRecordCount)
		if err != nil {
			return 0, errors.Wrap(err, "couldn't update the count of record")
		}
	}

	return updatedRecordCount.AsInt(), nil
}

var triggeredCountPrefix = []byte("$count_of_triggered_records$")

func (js *JoinedStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	addLeftRecords := js.joinType == LEFT_JOIN || js.joinType == OUTER_JOIN
	addRightRecords := js.joinType == OUTER_JOIN

	// Read the number of records already triggered from this JoinedStream. These will be used in assigning IDs to new records
	triggeredCountState := storage.NewValueState(tx.WithPrefix(triggeredCountPrefix))
	var triggeredCount octosql.Value

	err := triggeredCountState.Get(&triggeredCount)
	if err == storage.ErrNotFound {
		triggeredCount = octosql.MakeInt(0)
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't read count of already triggered records")
	}

	alreadyTriggeredCount := triggeredCount.AsInt()

	// Read records from the left stream that were already present after the previous Trigger() call
	oldLeftRecords, err := readAllAndTransformIntoRecords(txByKey.WithPrefix(leftStreamOldRecordsPrefix))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read old records from the left stream")
	}

	// Read records from the right stream that were already present after the previous Trigger() call
	oldRightRecords, err := readAllAndTransformIntoRecords(txByKey.WithPrefix(rightStreamOldRecordsPrefix))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read old records from the right stream")
	}

	// Read records from the left stream that arrived after the previous Trigger() call
	newLeftRecords, err := readAllAndTransformIntoRecords(txByKey.WithPrefix(leftStreamNewRecordsPrefix))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read new records from the left stream")
	}

	// Read records from the right stream that arrived after the previous Trigger() call
	newRightRecords, err := readAllAndTransformIntoRecords(txByKey.WithPrefix(rightStreamNewRecordsPrefix))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read new records from the right stream")
	}

	// This offset will be used to assign IDs to records
	baseOffset := alreadyTriggeredCount

	var allRecordsToTrigger []*Record

	// First we merge new left records with both old and new right records
	leftMergedWithAllRights := createPairsOfRecords(newLeftRecords, append(oldRightRecords, newRightRecords...), baseOffset, js.streamID, js.eventTimeField)
	allRecordsToTrigger = append(allRecordsToTrigger, leftMergedWithAllRights...)
	baseOffset += len(leftMergedWithAllRights)

	// Now we merge new right records with old left records. In this way we now have oldLeft x newRight, newLeft x oldRight and newLeft x newRight, which
	// is exactly what we want, since we basically want to get (allLeft x allRight) - (oldLeft x oldRight), since these records were already triggered
	newRightsMergedWithOldLefts := createPairsOfRecords(oldLeftRecords, newRightRecords, baseOffset, js.streamID, js.eventTimeField)
	allRecordsToTrigger = append(allRecordsToTrigger, newRightsMergedWithOldLefts...)
	baseOffset += len(newRightsMergedWithOldLefts)

	// If we are adding single left records (LEFT or OUTER join) we add them and update the offset
	if addLeftRecords {
		leftRecordsRenamed := renameRecords(newLeftRecords, baseOffset, js.streamID)
		allRecordsToTrigger = append(allRecordsToTrigger, leftRecordsRenamed...)
		baseOffset += len(newLeftRecords)
	}

	// Same goes for right records (in OUTER join)
	if addRightRecords {
		rightRecordsRenamed := renameRecords(newRightRecords, baseOffset, js.streamID)
		allRecordsToTrigger = append(allRecordsToTrigger, rightRecordsRenamed...)
		baseOffset += len(newRightRecords)
	}

	// Now we have to handle merging newRecords and oldRecords. Basically we move records from the new set to the old one,
	// while keeping in mind which new records are retractions, and then we clear the new set. We do this for both left and right streams
	if err := mergeNewAndOldRecords(txByKey, newLeftRecords, leftStreamNewRecordsPrefix, leftStreamOldRecordsPrefix); err != nil {
		return nil, errors.Wrap(err, "couldn't move left new records to the old records set")
	}

	if err := mergeNewAndOldRecords(txByKey, newRightRecords, rightStreamNewRecordsPrefix, rightStreamOldRecordsPrefix); err != nil {
		return nil, errors.Wrap(err, "couldn't move right new records to the old records set")
	}

	// Update the value of number of triggered records
	newTriggeredCount := octosql.MakeInt(baseOffset)
	if err := triggeredCountState.Set(&newTriggeredCount); err != nil {
		return nil, errors.Wrap(err, "couldn't update count of triggered records")
	}

	return allRecordsToTrigger, nil
}

func recordToValue(rec *Record) octosql.Value {
	fields := make([]octosql.Value, len(rec.FieldNames))
	data := make([]octosql.Value, len(rec.Data))
	eventTimeField := octosql.MakeString(rec.EventTimeField().String())

	for i := range rec.FieldNames {
		fields[i] = octosql.MakeString(rec.FieldNames[i])
		data[i] = *rec.Data[i]
	}

	valuesTogether := make([]octosql.Value, 4)

	valuesTogether[0] = octosql.MakeTuple(fields)
	valuesTogether[1] = octosql.MakeTuple(data)
	valuesTogether[2] = octosql.MakeBool(rec.IsUndo())
	valuesTogether[3] = eventTimeField

	return octosql.MakeTuple(valuesTogether)
}

func readAllAndTransformIntoRecords(tx storage.StateTransaction) ([]*Record, error) {
	set := storage.NewMultiSet(tx)

	recordValues, err := set.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read values from set")
	}

	records := make([]*Record, len(recordValues))

	for i, recordValue := range recordValues {
		records[i] = valueToRecord(recordValue)
	}

	return records, nil
}

func valueToRecord(val octosql.Value) *Record {
	slice := val.AsSlice()

	fieldNameValues := slice[0].AsSlice()
	values := slice[1].AsSlice()
	isUndo := slice[2].AsBool()
	eventTimeField := octosql.NewVariableName(slice[3].AsString())

	fieldNames := make([]octosql.VariableName, len(fieldNameValues))

	for i, fieldName := range fieldNameValues {
		fieldNames[i] = octosql.NewVariableName(fieldName.AsString())
	}

	if isUndo {
		return NewRecordFromSlice(fieldNames, values, WithEventTimeField(eventTimeField), WithUndo())
	}
	return NewRecordFromSlice(fieldNames, values, WithEventTimeField(eventTimeField), WithNoUndo())
}

func createPairsOfRecords(leftRecords, rightRecords []*Record, baseOffset int, streamID *StreamID, eventTimeField octosql.VariableName) []*Record {
	records := make([]*Record, len(leftRecords)*len(rightRecords))
	recordCount := 0

	for i := range leftRecords {
		leftRecord := leftRecords[i]

		for j := range rightRecords {
			rightRecord := rightRecords[j]

			// Get ID for new match based on the amount of records that were already triggered
			recordMatchID := NewRecordIDFromStreamIDWithOffset(streamID, baseOffset+recordCount)

			// A match is a retraction if any of the original records are a retraction
			isMatchUndo := leftRecord.IsUndo() || rightRecord.IsUndo()

			// Create and store match; increase the count of records created
			recordMatch := mergeRecords(leftRecord, rightRecord, recordMatchID, isMatchUndo, eventTimeField)
			records[recordCount] = recordMatch
			recordCount++
		}
	}

	return records
}

func mergeRecords(left, right *Record, ID *RecordID, isUndo bool, eventTimeField octosql.VariableName) *Record {
	mergedFieldNames := octosql.StringsToVariableNames(append(left.FieldNames, right.FieldNames...))
	mergedDataPointers := append(left.Data, right.Data...)

	mergedData := make([]octosql.Value, len(mergedDataPointers))

	for i := range mergedDataPointers {
		mergedData[i] = *mergedDataPointers[i]
	}

	// If eventTimeField is not empty, then we are joining by event_time and thus both records have the same value
	if eventTimeField != "" {
		eventTime := left.EventTime()
		mergedFieldNames = append(mergedFieldNames, eventTimeField)
		mergedData = append(mergedData, eventTime)
	}

	if isUndo {
		return NewRecordFromSlice(mergedFieldNames, mergedData, WithID(ID), WithUndo(), WithEventTimeField(eventTimeField))
	}
	return NewRecordFromSlice(mergedFieldNames, mergedData, WithID(ID), WithNoUndo(), WithEventTimeField(eventTimeField))
}

func renameRecords(records []*Record, baseOffset int, streamID *StreamID) []*Record {
	newRecords := make([]*Record, len(records))

	for i := range records {
		IDForRecord := NewRecordIDFromStreamIDWithOffset(streamID, baseOffset+i)
		newRecords[i] = NewRecordFromRecord(records[i], WithID(IDForRecord))
	}

	return newRecords
}

func mergeNewAndOldRecords(tx storage.StateTransaction, newRecords []*Record, newRecordsPrefix, oldRecordsPrefix []byte) error {
	oldRecordsSet := storage.NewMultiSet(tx.WithPrefix(oldRecordsPrefix))

	for i := range newRecords {
		record := newRecords[i]

		isRetraction := record.IsUndo()
		WithNoUndo()(record) // Reset the retraction value

		recordValue := recordToValue(record)

		if !isRetraction {
			// If the record isn't a retraction we simply store it
			if err := oldRecordsSet.Insert(recordValue); err != nil {
				return errors.Wrap(err, "couldn't insert new record into old records set")
			}
		} else {
			// Otherwise we remove one copy of this record (it should be present, since we handled counts in AddRecord)
			present, err := oldRecordsSet.Contains(recordValue)
			if err != nil {
				return errors.Wrap(err, "couldn't check if record is present in the old records set")
			}
			if !present { // this should never happen, but if it does, something went wrong
				panic("A record we are retracting isn't present in a set in JoinedStream.Trigger()")
			}

			if err := oldRecordsSet.Erase(recordValue); err != nil {
				return errors.Wrap(err, "couldn't remove record from the old records set")
			}
		}
	}

	newRecordsSet := storage.NewMultiSet(tx.WithPrefix(newRecordsPrefix))
	if err := newRecordsSet.Clear(); err != nil {
		return errors.Wrap(err, "couldn't clear new records set")
	}

	return nil
}

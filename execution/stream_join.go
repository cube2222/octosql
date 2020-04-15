package execution

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	LEFT  = 0
	RIGHT = 1
)

type StreamJoin struct {
	keys           [][]Expression // keys[0] are the expressions for the left source, keys[1] for the right source
	sources        []Node         // sources[0] = left source, sources[1] = right source
	storage        storage.Storage
	eventTimeField octosql.VariableName
	trigger        TriggerPrototype
}

func NewStreamJoin(leftKey, rightKey []Expression, sources []Node, storage storage.Storage, eventTimeField octosql.VariableName, trigger TriggerPrototype) *StreamJoin {
	return &StreamJoin{
		keys:           [][]Expression{leftKey, rightKey},
		sources:        sources,
		storage:        storage,
		eventTimeField: eventTimeField,
		trigger:        trigger,
	}
}

func (node *StreamJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	leftStream, leftExec, err := node.sources[LEFT].Get(ctx, variables, streamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get left source stream in stream join")
	}

	rightStream, rightExec, err := node.sources[RIGHT].Get(ctx, variables, streamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get right source stream in stream join")
	}

	execOutput := &UnifiedStream{
		sources:          []RecordStream{leftStream, rightStream},
		watermarkSources: []WatermarkSource{leftExec.WatermarkSource, rightExec.WatermarkSource},
		streamID:         sourceStreamID, // TODO: is this right?
	}

	trigger, err := node.trigger.Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create trigger for stream join")
	}

	stream := &JoinedStream{
		streamID: sourceStreamID,
	}

	processFunc := &ProcessByKey{
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpressions:  node.keys,
		processFunction: stream,
		variables:       variables,
	}

	streamJoinPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{leftStream, rightStream}, streamID, execOutput)
	go streamJoinPullEngine.Run(ctx)

	return streamJoinPullEngine, NewExecutionOutput(streamJoinPullEngine), nil
}

type JoinedStream struct {
	streamID *StreamID
}

var leftStreamRecordsPrefix = []byte("$left_stream_records$")
var rightStreamRecordsPrefix = []byte("$right_stream_records$")
var toTriggerPrefix = []byte("$values_to_trigger$")

func (js *JoinedStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex < 0 || inputIndex > 1 {
		panic("invalid inputIndex for stream join")
	}

	if record.Metadata.Id == nil {
		panic("no ID for record in stream join")
	}

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	isLeft := inputIndex == LEFT    // from which input source the record was received
	isRetraction := record.IsUndo() // is the incoming record a retraction

	// We get the key of the new incoming record
	newRecordValue := recordToValue(record)
	newRecordKey, err := proto.Marshal(&newRecordValue)
	if err != nil {
		return errors.Wrap(err, "couldn't marshall new record")
	}

	var myRecordSet, otherRecordSet *storage.Set

	// Important note:
	// 1) If we are adding the record it's natural that we want to match it with every record from the other stream
	// 2) If we are retracting, we want to also retract every matching between the new record and records from the other stream.
	// 	  This is because if records arrive in order R1, L1, L2, R2, R3 and then we retract L1, it has matched
	//	  with R1 (when L1 was added) and with R2 and R3 (when they were added)
	if isLeft {
		myRecordSet = storage.NewSet(txByKey.WithPrefix(leftStreamRecordsPrefix))
		otherRecordSet = storage.NewSet(txByKey.WithPrefix(rightStreamRecordsPrefix))
	} else {
		myRecordSet = storage.NewSet(txByKey.WithPrefix(rightStreamRecordsPrefix))
		otherRecordSet = storage.NewSet(txByKey.WithPrefix(leftStreamRecordsPrefix))
	}

	// Keep track of the count
	newCount, err := keepTrackOfCount(txByKey.WithPrefix(newRecordKey), isRetraction)
	if err != nil {
		return err
	}

	// If we got an early retraction, or a record arrived after its retraction then we do nothing
	if newCount < 0 || (newCount == 0 && !isRetraction) {
		return nil
	}

	// We add/remove the record to/from the appropriate set of records
	if !isRetraction {
		_, err = myRecordSet.Insert(newRecordValue)
		if err != nil {
			return errors.Wrap(err, "couldn't store record in its set")
		}
	} else {
		_, err = myRecordSet.Erase(newRecordValue)
		if err != nil {
			return errors.Wrap(err, "couldn't remove record from its set")
		}
	}

	// We read all the records from the other stream
	otherRecordValues, err := otherRecordSet.ReadAll()
	if err != nil {
		return errors.Wrap(err, "couldn't read records from the other stream")
	}

	toTriggerSet := storage.NewSet(txByKey.WithPrefix(toTriggerPrefix))

	for _, otherRecordValue := range otherRecordValues {
		// We will be creating matches between records. A match is a pair of records and information whether it's a retraction
		// or not. To make sure we never mix the order the record from the left stream always goes first
		var leftRecord, rightRecord octosql.Value
		if isLeft {
			leftRecord = newRecordValue
			rightRecord = otherRecordValue
		} else {
			leftRecord = otherRecordValue
			rightRecord = newRecordValue
		}

		match := createMatch(leftRecord, rightRecord, false)

		if !isRetraction { // adding the record
			_, err = toTriggerSet.Insert(match)
			if err != nil {
				return errors.Wrap(err, "couldn't store a match of records")
			}
		} else { // retracting a record
			// Now there are two possibilities:
			// 1) The matching is present in the toTrigger set -> we just remove it
			// 2) The matching isn't present (so it's already been triggered) -> we send a retraction for it

			isPresent, err := toTriggerSet.Contains(match)
			if err != nil {
				return errors.Wrap(err, "couldn't check whether a match of records is present in the toTrigger set")
			}

			if isPresent {
				_, err = toTriggerSet.Erase(match)
				if err != nil {
					return errors.Wrap(err, "couldn't remove a match of records from the toTrigger set")
				}
			} else {
				_, err = toTriggerSet.Insert(createMatch(leftRecord, rightRecord, true)) // the retraction
				if err != nil {
					return errors.Wrap(err, "couldn't store the retraction of a match ")
				}
			}
		}
	}

	return nil
}

var triggeredCountPrefix = []byte("$count_of_triggered_records$")

func (js *JoinedStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	triggeredCountState := storage.NewValueState(tx.WithPrefix(triggeredCountPrefix))
	var triggeredCount octosql.Value

	err := triggeredCountState.Get(&triggeredCount)
	if err == storage.ErrNotFound {
		triggeredCount = octosql.MakeInt(0)
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't read count of already triggered records")
	}

	triggeredCountValue := triggeredCount.AsInt()

	// Read all the pairs to merge and send
	toTriggerSet := storage.NewSet(txByKey.WithPrefix(toTriggerPrefix))
	matches, err := toTriggerSet.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read record matches to trigger")
	}

	// Update the value of number of triggered records
	newTriggeredCount := octosql.MakeInt(triggeredCountValue + len(matches))

	err = triggeredCountState.Set(&newTriggeredCount)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't update count of triggered records")
	}

	records := make([]*Record, 0)
	// Retrieve record pairs from the set and merge them
	for i, match := range matches {
		idForMatch := NewRecordIDFromStreamIDWithOffset(js.streamID, triggeredCountValue+i)
		leftRecordValue, rightRecordValue, isUndo := unwrapMatch(match)

		leftRecord := valueToRecord(leftRecordValue)
		rightRecord := valueToRecord(rightRecordValue)

		mergedRecord := mergeRecords(leftRecord, rightRecord, idForMatch, isUndo)
		records = append(records, mergedRecord)
	}

	err = toTriggerSet.Clear()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't clear the toTrigger set")
	}

	return records, nil
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

func recordToValue(rec *Record) octosql.Value {
	fields := make([]octosql.Value, len(rec.FieldNames))
	data := make([]octosql.Value, len(rec.Data))

	IDValue := octosql.MakeString(rec.ID().ID)
	undoValue := octosql.MakeBool(rec.IsUndo())
	eventTimeField := octosql.MakeString(rec.EventTimeField().String())

	for i := range rec.FieldNames {
		fields[i] = octosql.MakeString(rec.FieldNames[i])
		data[i] = *rec.Data[i]
	}

	valuesTogether := make([]octosql.Value, 5)

	valuesTogether[0] = octosql.MakeTuple(fields)
	valuesTogether[1] = octosql.MakeTuple(data)
	valuesTogether[2] = IDValue
	valuesTogether[3] = undoValue
	valuesTogether[4] = eventTimeField

	return octosql.MakeTuple(valuesTogether)
}

func valueToRecord(val octosql.Value) *Record {
	slice := val.AsSlice()

	fieldNameValues := slice[0].AsSlice()
	values := slice[1].AsSlice()
	ID := slice[2].AsString()
	undo := slice[3].AsBool()
	eventTimeField := octosql.NewVariableName(slice[4].AsString())

	fieldNames := make([]octosql.VariableName, len(fieldNameValues))

	for i, fieldName := range fieldNameValues {
		fieldNames[i] = octosql.NewVariableName(fieldName.AsString())
	}

	var record *Record

	if undo {
		record = NewRecordFromSlice(fieldNames, values, WithID(NewRecordID(ID)), WithEventTimeField(eventTimeField), WithUndo())
	} else {
		record = NewRecordFromSlice(fieldNames, values, WithID(NewRecordID(ID)), WithEventTimeField(eventTimeField), WithNoUndo())
	}

	return record
}

// We create a octosql.Value from two records (as values), a flag whether the match is a retraction,
// and an int indicating which of the records came later
func createMatch(first, second octosql.Value, isUndo bool) octosql.Value {
	return octosql.MakeTuple([]octosql.Value{first, second, octosql.MakeBool(isUndo)})
}

func unwrapMatch(match octosql.Value) (octosql.Value, octosql.Value, bool) {
	asSlice := match.AsSlice()
	return asSlice[0], asSlice[1], asSlice[2].AsBool()
}

func mergeRecords(left, right *Record, ID *RecordID, isUndo bool) *Record {
	mergedFieldNames := octosql.StringsToVariableNames(append(left.FieldNames, right.FieldNames...))
	mergedDataPointers := append(left.Data, right.Data...)

	mergedData := make([]octosql.Value, len(mergedDataPointers))

	for i := range mergedDataPointers {
		mergedData[i] = *mergedDataPointers[i]
	}

	var eventTimeField octosql.VariableName
	leftEventTimeField := left.EventTimeField()
	rightEventTimeField := right.EventTimeField()

	if leftEventTimeField != "" && rightEventTimeField != "" {
		leftTime := left.EventTime().AsTime()
		rightTime := right.EventTime().AsTime()

		if leftTime.After(rightTime) {
			eventTimeField = leftEventTimeField
		} else {
			eventTimeField = rightEventTimeField
		}
	} else if leftEventTimeField == "" {
		eventTimeField = rightEventTimeField
	} else {
		eventTimeField = leftEventTimeField
	}

	if isUndo {
		return NewRecordFromSlice(mergedFieldNames, mergedData, WithID(ID), WithUndo(), WithEventTimeField(eventTimeField))
	} else {
		return NewRecordFromSlice(mergedFieldNames, mergedData, WithID(ID), WithNoUndo(), WithEventTimeField(eventTimeField))
	}
}

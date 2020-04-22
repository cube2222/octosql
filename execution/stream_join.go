package execution

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type JoinType int

const (
	INNER_JOIN JoinType = 0
	LEFT_JOIN  JoinType = 1
	OUTER_JOIN JoinType = 2
)

const (
	LEFT  = 0
	RIGHT = 1
)

type StreamJoin struct {
	leftKey        []Expression
	rightKey       []Expression
	leftSource     Node
	rightSource    Node
	storage        storage.Storage
	eventTimeField octosql.VariableName
	joinType       JoinType
	trigger        TriggerPrototype
}

// As with distinct, triggering a stream join is heavily dependant on triggering its sources
// so sticking with a counting trigger 1 is the easiest approach
func NewStreamJoin(leftSource, rightSource Node, leftKey, rightKey []Expression, storage storage.Storage, eventTimeField octosql.VariableName, joinType JoinType) *StreamJoin {
	return &StreamJoin{
		leftKey:        leftKey,
		rightKey:       rightKey,
		leftSource:     leftSource,
		rightSource:    rightSource,
		storage:        storage,
		eventTimeField: eventTimeField,
		joinType:       joinType,
		trigger:        NewCountingTrigger(NewDummyValue(octosql.MakeInt(1))),
	}
}

func (node *StreamJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
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

	execOutput := &UnifiedStream{
		sources:          []RecordStream{leftStream, rightStream},
		watermarkSources: []WatermarkSource{leftExec.WatermarkSource, rightExec.WatermarkSource},
		streamID:         GetRawStreamID(), // TODO: is this right?
	}

	trigger, err := node.trigger.Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create trigger for stream join")
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

	streamJoinPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{leftStream, rightStream}, streamID, execOutput)
	go streamJoinPullEngine.Run(ctx)

	return streamJoinPullEngine, NewExecutionOutput(streamJoinPullEngine), nil
}

type JoinedStream struct {
	streamID       *StreamID
	eventTimeField octosql.VariableName
	joinType       JoinType
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

	// Keep track of the count. Since recordKey contains ID this will basically be either -1, 0 or 1
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
		if err := myRecordSet.Insert(newRecordValue); err != nil {
			return errors.Wrap(err, "couldn't store record in its set")
		}
	} else {
		if err := myRecordSet.Erase(newRecordValue); err != nil {
			return errors.Wrap(err, "couldn't remove record from its set")
		}
	}

	// We read all the records from the other stream
	otherRecordValues, err := otherRecordSet.ReadAll()
	if err != nil {
		return errors.Wrap(err, "couldn't read records from the other stream")
	}

	toTriggerSet := storage.NewSet(txByKey.WithPrefix(toTriggerPrefix))

	// Add matched records
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
			if err := toTriggerSet.Insert(match); err != nil {
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
				if err := toTriggerSet.Erase(match); err != nil {
					return errors.Wrap(err, "couldn't remove a match of records from the toTrigger set")
				}
			} else {
				if err := toTriggerSet.Insert(createMatch(leftRecord, rightRecord, true)); err != nil {
					return errors.Wrap(err, "couldn't store the retraction of a match ")
				}
			}
		}
	}

	// Now additionally we might need to add the record itself if we are dealing with a specific join type

	// If we are performing an outer join or a left join and the record comes from the left
	if js.joinType == OUTER_JOIN || (js.joinType == LEFT_JOIN && isLeft) {
		if !isRetraction {
			if err := toTriggerSet.Insert(newRecordValue); err != nil {
				return errors.Wrap(err, "couldn't insert record into the toTrigger set")
			}
		} else {
			// If it's a retraction we do the same as before: check if it's present in the set, if yes
			// remove it, otherwise send a retraction.
			contains, err := toTriggerSet.Contains(newRecordValue)
			if err != nil {
				return errors.Wrap(err, "couldn't check whether the toTrigger set contains the record")
			}

			if contains {
				// Remove the record from the set
				if err := toTriggerSet.Erase(newRecordValue); err != nil {
					return errors.Wrap(err, "couldn't remove record from the toTrigger set")
				}
			} else {
				// Send a retraction
				retractionRecord := NewRecordFromRecord(record, WithUndo())
				retractionRecordValue := recordToValue(retractionRecord)

				if err := toTriggerSet.Insert(retractionRecordValue); err != nil {
					return errors.Wrap(err, "couldn't insert a retraction for the record into the toTrigger set")
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

		mergedRecord := mergeRecords(leftRecord, rightRecord, idForMatch, isUndo, js.eventTimeField)
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
	} else {
		return NewRecordFromSlice(mergedFieldNames, mergedData, WithID(ID), WithNoUndo(), WithEventTimeField(eventTimeField))
	}
}

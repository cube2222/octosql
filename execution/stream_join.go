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

	streamJoinPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{leftStream, rightStream}, streamID, watermarkSource, true)

	return streamJoinPullEngine,
		NewExecutionOutput(
			streamJoinPullEngine,
			mergedNextShuffles,
			append(leftExec.TasksToRun, append(rightExec.TasksToRun, func() error { streamJoinPullEngine.Run(ctx); return nil })...),
		), nil
}

type JoinedStream struct {
	streamID       *StreamID
	eventTimeField octosql.VariableName
	joinType       JoinType
}

var leftStreamRecordsPrefix = []byte("$left_stream_records$")
var rightStreamRecordsPrefix = []byte("$right_stream_records$")
var matchesToTriggerPrefix = []byte("$matches_to_trigger$")
var recordsToTriggerPrefix = []byte("$records_to_trigger$")

func (js *JoinedStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex < 0 || inputIndex > 1 {
		panic("invalid inputIndex for stream join")
	}

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	isLeft := inputIndex == 0       // from which input source the record was received
	isRetraction := record.IsUndo() // is the incoming record a retraction

	// We get the key of the new incoming record
	newRecordValue := recordToValue(record)
	newRecordKey, err := proto.Marshal(&newRecordValue)
	if err != nil {
		return errors.Wrap(err, "couldn't marshall new record")
	}

	var myRecordSet, otherRecordSet *storage.MultiSet

	// Important note:
	// 1) If we are adding the record it's natural that we want to match it with every record from the other stream
	// 2) If we are retracting, we want to also retract every matching between the new record and records from the other stream.
	// 	  This is because if records arrive in order R1, L1, L2, R2, R3 and then we retract L1, it has matched
	//	  with R1 (when L1 was added) and with R2 and R3 (when they were added)
	if isLeft {
		myRecordSet = storage.NewMultiSet(txByKey.WithPrefix(leftStreamRecordsPrefix))
		otherRecordSet = storage.NewMultiSet(txByKey.WithPrefix(rightStreamRecordsPrefix))
	} else {
		myRecordSet = storage.NewMultiSet(txByKey.WithPrefix(rightStreamRecordsPrefix))
		otherRecordSet = storage.NewMultiSet(txByKey.WithPrefix(leftStreamRecordsPrefix))
	}

	// Keep track of the count of the record (fields + data no metadata)
	newCount, err := keepTrackOfCount(txByKey.WithPrefix(newRecordKey), isRetraction)
	if err != nil {
		return err
	}

	// If we got an early retraction, or a record bumped up the count to 0, but it was previously retracted we do nothing
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

	matchesToTriggerSet := storage.NewMultiSet(txByKey.WithPrefix(matchesToTriggerPrefix))

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
			if err := matchesToTriggerSet.Insert(match); err != nil {
				return errors.Wrap(err, "couldn't store a match of records")
			}
		} else { // retracting a record
			// Now there are two possibilities:
			// 1) The matching is present in the toTrigger set -> we just remove it
			// 2) The matching isn't present (so it's already been triggered) -> we send a retraction for it

			isPresent, err := matchesToTriggerSet.Contains(match)
			if err != nil {
				return errors.Wrap(err, "couldn't check whether a match of records is present in the matched records set")
			}

			if isPresent {
				if err := matchesToTriggerSet.Erase(match); err != nil {
					return errors.Wrap(err, "couldn't remove a match of records from the matched records set")
				}
			} else {
				if err := matchesToTriggerSet.Insert(createMatch(leftRecord, rightRecord, true)); err != nil {
					return errors.Wrap(err, "couldn't store the retraction of a match in the matched records set")
				}
			}
		}
	}

	// Now additionally we might need to add the record itself if we are dealing with a specific join type

	recordsToTriggerSet := storage.NewMultiSet(txByKey.WithPrefix(recordsToTriggerPrefix))

	// If we are performing an outer join or a left join and the record comes from the left
	if js.joinType == OUTER_JOIN || (js.joinType == LEFT_JOIN && isLeft) {
		if !isRetraction {
			if err := recordsToTriggerSet.Insert(newRecordValue); err != nil {
				return errors.Wrap(err, "couldn't insert record into the single records set")
			}
		} else {
			// If it's a retraction we do the same as before: check if it's present in the set, if yes
			// remove it, otherwise send a retraction.
			contains, err := recordsToTriggerSet.Contains(newRecordValue)
			if err != nil {
				return errors.Wrap(err, "couldn't check whether the single records set contains the record")
			}

			if contains {
				// Remove the record from the set
				if err := recordsToTriggerSet.Erase(newRecordValue); err != nil {
					return errors.Wrap(err, "couldn't remove record from the single records set")
				}
			} else {
				// Send a retraction
				retractionRecord := NewRecordFromRecord(record, WithMetadataFrom(record), WithUndo())
				retractionRecordValue := recordToValue(retractionRecord)

				if err := recordsToTriggerSet.Insert(retractionRecordValue); err != nil {
					return errors.Wrap(err, "couldn't insert a retraction for the record into the single records set")
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
	matchesToTriggerSet := storage.NewMultiSet(txByKey.WithPrefix(matchesToTriggerPrefix))
	matches, err := matchesToTriggerSet.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read record matches to trigger")
	}

	// Update the value of number of triggered records
	newTriggeredCount := octosql.MakeInt(triggeredCountValue + len(matches))

	if err := triggeredCountState.Set(&newTriggeredCount); err != nil {
		return nil, errors.Wrap(err, "couldn't update count of triggered records")
	}

	records := make([]*Record, len(matches))
	// Retrieve record pairs from the set and merge them
	for i, match := range matches {
		idForMatch := NewRecordIDFromStreamIDWithOffset(js.streamID, triggeredCountValue+i)
		leftRecordValue, rightRecordValue, isUndo := unwrapMatch(match)

		leftRecord := valueToRecord(leftRecordValue)
		rightRecord := valueToRecord(rightRecordValue)

		mergedRecord := mergeRecords(leftRecord, rightRecord, idForMatch, isUndo, js.eventTimeField)
		records[i] = mergedRecord
	}

	if err := matchesToTriggerSet.Clear(); err != nil {
		return nil, errors.Wrap(err, "couldn't clear the set of matches")
	}

	// If it's not an inner join we might need to send single, not matched records
	if js.joinType != INNER_JOIN {
		triggeredCountValue += len(matches)

		recordsToTriggerSet := storage.NewMultiSet(txByKey.WithPrefix(recordsToTriggerPrefix))

		singleRecords, err := recordsToTriggerSet.ReadAll()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't read single records from record set")
		}

		for i := range singleRecords {
			singleRecord := valueToRecord(singleRecords[i])
			idForRecord := NewRecordIDFromStreamIDWithOffset(js.streamID, triggeredCountValue+i)
			WithID(idForRecord)(singleRecord)

			records = append(records, singleRecord)
		}

		if err := recordsToTriggerSet.Clear(); err != nil {
			return nil, errors.Wrap(err, "couldn't clear the set of single records")
		}

		newTriggeredCount = octosql.MakeInt(triggeredCountValue + len(singleRecords))

		if err := triggeredCountState.Set(&newTriggeredCount); err != nil {
			return nil, errors.Wrap(err, "couldn't update count of triggered records after triggering single records")
		}
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
	eventTimeField := octosql.MakeString(rec.EventTimeField().String())

	for i := range rec.FieldNames {
		fields[i] = octosql.MakeString(rec.FieldNames[i])
		data[i] = *rec.Data[i]
	}

	valuesTogether := make([]octosql.Value, 3)

	valuesTogether[0] = octosql.MakeTuple(fields)
	valuesTogether[1] = octosql.MakeTuple(data)
	valuesTogether[2] = eventTimeField

	return octosql.MakeTuple(valuesTogether)
}

func valueToRecord(val octosql.Value) *Record {
	slice := val.AsSlice()

	fieldNameValues := slice[0].AsSlice()
	values := slice[1].AsSlice()
	eventTimeField := octosql.NewVariableName(slice[2].AsString())

	fieldNames := make([]octosql.VariableName, len(fieldNameValues))

	for i, fieldName := range fieldNameValues {
		fieldNames[i] = octosql.NewVariableName(fieldName.AsString())
	}

	return NewRecordFromSlice(fieldNames, values, WithEventTimeField(eventTimeField))
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

package execution

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
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

	stream := &JoinedStream{}

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

type JoinedStream struct{}

var leftStreamRecordsPrefix = []byte("$left_stream_records$")
var rightStreamRecordsPrefix = []byte("$right_stream_records$")
var totalRecordCountPrefix = []byte("$total_record_count$")

func (js *JoinedStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex < 0 || inputIndex > 1 {
		panic("invalid inputIndex for stream join")
	}

	if record.Metadata.Id == nil {
		panic("no ID for record in stream join")
	}

	isLeft := inputIndex == 0 // from which input source the record was received

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	if !record.IsUndo() { // we add the record
		var myRecordSet, otherRecordSet *storage.Set

		if isLeft {
			myRecordSet = storage.NewSet(txByKey.WithPrefix(leftStreamRecordsPrefix))
			otherRecordSet = storage.NewSet(txByKey.WithPrefix(rightStreamRecordsPrefix))
		} else {
			myRecordSet = storage.NewSet(txByKey.WithPrefix(rightStreamRecordsPrefix))
			otherRecordSet = storage.NewSet(txByKey.WithPrefix(leftStreamRecordsPrefix))
		}

		// We save the record in the appropriate record set
		// Note that if two exact same records come in (with same columns and data) they will be separate
		// since we also include the metadata, and hence the ID
		_, err := myRecordSet.Insert(recordToValue(record))
		if err != nil {
			return errors.Wrap(err, "couldn't store record")
		}

		// Now get all records from the other stream
		otherRecordValues, err := otherRecordSet.ReadAll()
		if err != nil {
			return errors.Wrap(err, "couldn't read records from the other stream")
		}

		otherRecords := make([]*Record, len(otherRecordValues))
		for i, recordValue := range otherRecordValues {
			otherRecords[i] = valueToRecord(recordValue)
		}

		// Now we have the record that just arrived, and all the records that it matches with, so we merge them

		// First we check how many records we sent already through the stream join and we update this value
		sentRecordsCountState := storage.NewValueState(tx.WithPrefix(totalRecordCountPrefix))
		var sentRecordsCount octosql.Value

		err = sentRecordsCountState.Get(&sentRecordsCount)
		if err == storage.ErrNotFound {
			sentRecordsCount = octosql.MakeInt(0)
		} else if err != nil {
			return errors.Wrap(err, "couldn't check the number of records previously sent")
		}

		// Now we have the number of records we already sent, so we increase it by the new records and store it
		recordsSentBefore := sentRecordsCount.AsInt()
		recordsSentUpdated := octosql.MakeInt(recordsSentBefore + len(otherRecords))

		err = sentRecordsCountState.Set(&recordsSentUpdated)
		if err != nil {
			return errors.Wrap(err, "couldn't save the new number of records sent through")
		}

	} else { // we retract the record

	}

	return nil
}

func (js *JoinedStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	return nil, nil
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

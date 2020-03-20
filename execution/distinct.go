package execution

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

type Distinct struct {
	storage storage.Storage
	source  Node
}

func NewDistinct(storage storage.Storage, source Node) *Distinct {
	return &Distinct{
		storage: storage,
		source:  source,
	}
}

func (node *Distinct) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, execOutput, err := node.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get stream for source node in distinct")
	}

	trigger, err := NewCountingTrigger(NewDummyValue(octosql.MakeInt(1))).Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create trigger for distinct")
	}

	distinct := &DistinctStream{
		variables: variables,
	}

	processFunc := &ProcessByKey{
		eventTimeField:  "", // TODO: what about this?
		trigger:         trigger,
		keyExpression:   []Expression{&RecordExpression{}},
		processFunction: distinct,
		variables:       variables,
	}

	distinctPullEngine := NewPullEngine(processFunc, node.storage, source, streamID, execOutput.WatermarkSource)
	go distinctPullEngine.Run(ctx) // TODO: .Close() should kill this context and the goroutine.

	return distinctPullEngine, NewExecutionOutput(distinctPullEngine), nil
}

type DistinctStream struct {
	variables octosql.Variables
}

func (ds *DistinctStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex > 0 {
		panic("only one input stream allowed for group by")
	}
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	// Keep track of record vs retraction count
	recordCountState := storage.NewValueState(txByKey.WithPrefix(recordCountPrefix))
	var recordCount octosql.Value
	err := recordCountState.Get(&recordCount)
	if err == storage.ErrNotFound {
		recordCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current record count")
	}

	var newRecordCount int
	if !record.IsUndo() {
		newRecordCount = recordCount.AsInt() + 1
	} else {
		newRecordCount = recordCount.AsInt() - 1
	}

	if newRecordCount != 0 {
		newRecordCountValue := octosql.MakeInt(newRecordCount)
		err := recordCountState.Set(&newRecordCountValue)
		if err != nil {
			return errors.Wrap(err, "couldn't save record count")
		}
	} else {
		err := recordCountState.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear record count")
		}
	}

	return nil
}

func (ds *DistinctStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	phantom := octosql.MakePhantom()

	// Check whether record was triggered
	recordTriggerState := storage.NewValueState(txByKey.WithPrefix(previouslyTriggeredValuePrefix))
	var wasRecordTriggered bool

	err := recordTriggerState.Get(&phantom)
	if err == nil {
		wasRecordTriggered = true
	} else if err == storage.ErrNotFound {
		wasRecordTriggered = false
	} else {
		return nil, errors.Wrap(err, "couldn't check whether record was already triggered")
	}

	keyTuple := key.AsSlice()
	if len(keyTuple) == 0 { // empty record
		return nil, nil
	}

	recordMap := keyTuple[0].AsMap()

	// Check whether record is present
	recordCountState := storage.NewValueState(txByKey.WithPrefix(recordCountPrefix))
	var isRecordPresent bool

	err = recordCountState.Get(&phantom)
	if err == nil {
		isRecordPresent = true
	} else if err == storage.ErrNotFound {
		isRecordPresent = false
	} else {
		return nil, errors.Wrap(err, "couldn't check whether record is present in state")
	}

	if !wasRecordTriggered && isRecordPresent {
		// This is the first time that record is triggered and it is present, so we send it

		// Save that the record was triggered
		err = recordTriggerState.Set(&phantom)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't mark record as retracted")
		}

		record := recordFromMap(recordMap, false)
		return []*Record{record}, nil

	} else if wasRecordTriggered && !isRecordPresent {
		// The record was triggered, but it isn't present

		// Reset the triggering of the record
		err = recordTriggerState.Clear()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't reset that record was triggered")
		}

		// Send the retraction
		record := recordFromMap(recordMap, true)
		return []*Record{record}, nil
	}

	// Otherwise we the record was triggered and it's still present, so we don't do anything
	return nil, nil
}

// TODO: move this somewhere else?
func recordFromMap(m map[string]octosql.Value, isUndo bool) *Record {
	fields := make([]octosql.VariableName, len(m))
	values := make([]octosql.Value, len(m))

	i := 0
	for k, v := range m {
		fields[i] = octosql.NewVariableName(k)
		values[i] = v
		i++
	}

	if isUndo {
		return NewRecordFromSlice(fields, values, WithUndo())
	} else {
		return NewRecordFromSlice(fields, values, WithNoUndo())
	}
}

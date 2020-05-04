package execution

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type Distinct struct {
	storage        storage.Storage
	source         Node
	eventTimeField octosql.VariableName
}

func NewDistinct(storage storage.Storage, source Node, eventTimeField octosql.VariableName) *Distinct {
	return &Distinct{
		storage:        storage,
		source:         source,
		eventTimeField: eventTimeField,
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

	// The trigger for distinct is counting trigger with value 1, because triggering of Distinct
	// is strongly related to triggering of the underlying source, so that's the simplest approach
	trigger, err := NewCountingTrigger(NewConstantValue(octosql.MakeInt(1))).Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create trigger for distinct")
	}

	distinct := &DistinctStream{
		variables: variables,
	}

	processFunc := &ProcessByKey{
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpressions:  [][]Expression{{&RecordExpression{}}},
		processFunction: distinct,
		variables:       variables,
	}

	distinctPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{source}, streamID, execOutput.WatermarkSource, true, ctx)

	return distinctPullEngine,
		NewExecutionOutput(
			distinctPullEngine,
			execOutput.NextShuffles,
			append(execOutput.TasksToRun, func() error { distinctPullEngine.Run(); return nil }),
		),
		nil
}

type DistinctStream struct {
	variables octosql.Variables
}

var idStoragePrefix = []byte("$id_value_state$")
var recordStoragePrefix = []byte("$record_value_state$")
var distinctRecordCountPrefix = []byte("$record_count$")

func (ds *DistinctStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex > 0 {
		panic("only one input stream allowed for distinct")
	}

	if record.Metadata.Id == nil {
		panic("no id for record in distinct")
	}

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	// Keep track of record vs retraction count
	recordCountState := storage.NewValueState(txByKey.WithPrefix(distinctRecordCountPrefix))
	var recordCount octosql.Value

	err := recordCountState.Get(&recordCount)
	if err == storage.ErrNotFound {
		recordCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current record count")
	}

	var newRecordCount int
	wasRetracted := false

	if !record.IsUndo() {
		newRecordCount = recordCount.AsInt() + 1
	} else {
		newRecordCount = recordCount.AsInt() - 1
		wasRetracted = true
	}

	keyIDState := storage.NewValueState(txByKey.WithPrefix(idStoragePrefix))
	keyRecordState := storage.NewValueState(txByKey.WithPrefix(recordStoragePrefix))

	if newRecordCount != 0 {
		newRecordCountValue := octosql.MakeInt(newRecordCount)
		err := recordCountState.Set(&newRecordCountValue)
		if err != nil {
			return errors.Wrap(err, "couldn't save record count")
		}

		// If it's the first record with given key, we store the record
		if newRecordCount == 1 {
			err = keyRecordState.Set(record)
			if err != nil {
				return errors.Wrap(err, "couldn't save record")
			}
		}
	} else {
		err := recordCountState.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear record count")
		}

	}

	// Now we have two possible scenarios:
	// 1) There were no records for that specific key and now the first one arrived (the first case of the if)
	// That means we have to set the ID, because it will be used to trigger this key in Trigger().
	// 2) There was exactly one record for that specific key and now we retracted it, so we have to
	// retract it in Trigger(). We store the ID, since this ID will be used to retract in Trigger().
	// Notice that the two missing cases: (wasRetracted && newRecordCount == 1) and (!wasRetracted && newRecourdCount == 0)
	// mean respectively that we had two records, and we retracted one (so nothing happens) or
	// we had an early retraction and now we added a record, so nothing happens as well.

	recordID := octosql.MakeString(record.ID().ID)

	if (!wasRetracted && newRecordCount == 1) || (wasRetracted && newRecordCount == 0) {
		err := keyIDState.Set(&recordID)
		if err != nil {
			return errors.Wrap(err, "couldn't save records ID")
		}
	}

	return nil
}

var distinctPreviouslyTriggeredPrefix = []byte("$previously_triggered_value$")

func (ds *DistinctStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	// Check whether record was triggered
	recordTriggerState := storage.NewValueState(txByKey.WithPrefix(distinctPreviouslyTriggeredPrefix))
	var wasRecordTriggered bool

	phantom := octosql.MakePhantom()

	err := recordTriggerState.Get(&phantom)
	if err == nil {
		wasRecordTriggered = true
	} else if err == storage.ErrNotFound {
		wasRecordTriggered = false
	} else {
		return nil, errors.Wrap(err, "couldn't check whether record was already triggered")
	}

	// Check whether record is present
	recordCountState := storage.NewValueState(txByKey.WithPrefix(distinctRecordCountPrefix))
	var isRecordPresent bool
	var recordCount octosql.Value

	err = recordCountState.Get(&recordCount)
	if err == nil {
		isRecordPresent = recordCount.AsInt() > 0
	} else if err == storage.ErrNotFound {
		isRecordPresent = false
	} else {
		return nil, errors.Wrap(err, "couldn't check whether record is present in state")
	}

	// Get the ID for this key
	var recordID octosql.Value
	keyIDState := storage.NewValueState(txByKey.WithPrefix(idStoragePrefix))

	err = keyIDState.Get(&recordID)
	if err != nil && err != storage.ErrNotFound {
		return nil, errors.Wrap(err, "couldn't read ID for key")
	}

	keyRecordState := storage.NewValueState(txByKey.WithPrefix(recordStoragePrefix))
	var record Record

	err = keyRecordState.Get(&record)
	if err != nil && err != storage.ErrNotFound {
		return nil, errors.Wrap(err, "couldn't read record")
	}

	if !wasRecordTriggered && isRecordPresent {
		// This is the first time that record is triggered and it is present, so we send it

		// Save that the record was triggered
		err = recordTriggerState.Set(&phantom)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't mark record as triggered")
		}

		return []*Record{NewRecordFromRecord(&record, WithNoUndo(), WithID(NewRecordID(recordID.AsString())))}, nil
	} else if wasRecordTriggered && !isRecordPresent {
		// The record was triggered, but it isn't present, so we retract it
		// note that even if the recordCount is negative, it still means it should be retracted.

		// Reset the triggering of the record
		err = recordTriggerState.Clear()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't reset that record was triggered")
		}

		// The record was triggered but it was retracted so we have to clear its ID and the record here
		err = keyIDState.Clear()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't clear id for key")
		}

		err = keyRecordState.Clear()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't clear record for key")
		}

		// Send the retraction
		return []*Record{NewRecordFromRecord(&record, WithUndo(), WithID(NewRecordID(recordID.AsString())))}, nil

	}

	// Otherwise we the record was triggered and it's still present, so we don't do anything
	return nil, nil
}

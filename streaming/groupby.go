package streaming

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type Aggregate interface {
	AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error
	RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error
	GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error)
	String() string
}

// TODO: Physical plan nodes need to have the GetEventTimeField() method. There's no sense for a stream to be mixed anyways
// This way, we know before creation, if the used variable is an event time group by or not.
// This would be injected into any ProcessFunctions
type GroupBy struct {
	prefixes [][]byte

	eventTimeField octosql.VariableName // Empty if not grouping by event time
	inputFields    []octosql.VariableName
	aggregates     []Aggregate

	outputFieldNames []octosql.VariableName
}

var eventTimePrefix = []byte("$event_time$")

func (gb *GroupBy) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *execution.Record) error {
	if inputIndex > 0 {
		panic("only one input stream allowed for group by")
	}
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	for i := range gb.aggregates {
		var value octosql.Value
		if gb.inputFields[i] == "*star*" {
			mapping := make(map[string]octosql.Value, len(record.Fields()))
			for _, field := range record.Fields() {
				mapping[field.Name.String()] = record.Value(field.Name)
			}
			value = octosql.MakeObject(mapping)
		} else if gb.inputFields[i] == gb.eventTimeField && gb.aggregates[i].String() == "first" {
			eventTimeState := storage.NewValueState(txByKey.WithPrefix(eventTimePrefix))
			val := record.Value(gb.eventTimeField)
			err := eventTimeState.Set(&val)
			if err != nil {
				return errors.Wrap(err, "couldn't save event time")
			}
			continue
		} else {
			value = record.Value(gb.inputFields[i])
		}
		if !record.IsUndo() {
			err := gb.aggregates[i].AddValue(ctx, txByKey.WithPrefix(gb.prefixes[i]), value)
			if err != nil {
				return errors.Wrapf(
					err,
					"couldn't add record value to aggregate %s with index %v",
					gb.aggregates[i].String(),
					i,
				)
			}
		} else {
			err := gb.aggregates[i].RetractValue(ctx, txByKey.WithPrefix(gb.prefixes[i]), value)
			if err != nil {
				return errors.Wrapf(
					err,
					"couldn't retract record value from aggregate %s with index %v",
					gb.aggregates[i].String(),
					i,
				)
			}
		}
	}

	return nil
}

var previouslyTriggeredValuePrefix = []byte("$previously_triggered_value$")

func (gb *GroupBy) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*execution.Record, error) {
	output := make([]*execution.Record, 0, 2)
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	var opts []execution.RecordOption
	if len(gb.eventTimeField) > 0 {
		opts = append(opts, execution.WithEventTimeField(gb.eventTimeField))
	}

	previouslyTriggeredValues := storage.NewValueState(txByKey.WithPrefix(previouslyTriggeredValuePrefix))
	var previouslyTriggered octosql.Value
	err := previouslyTriggeredValues.Get(&previouslyTriggered)
	if err == nil {
		output = append(output, execution.NewRecordFromSlice(gb.outputFieldNames, previouslyTriggered.AsSlice(), append(opts[:len(opts):len(opts)], execution.WithUndo())...))
	} else if err != storage.ErrKeyNotFound {
		return nil, errors.Wrap(err, "couldn't get previously triggered value for key")
	}

	values := make([]octosql.Value, len(gb.aggregates))
	for i := range gb.aggregates {
		if gb.inputFields[i] == gb.eventTimeField && gb.aggregates[i].String() == "first" {
			eventTimeState := storage.NewValueState(txByKey.WithPrefix(eventTimePrefix))
			err := eventTimeState.Get(&values[i])
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get event time")
			}
			continue
		}
		var err error
		values[i], err = gb.aggregates[i].GetValue(ctx, txByKey.WithPrefix(gb.prefixes[i]))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get value of aggregate %s with index %d", gb.aggregates[i].String(), i)
		}
	}
	output = append(output, execution.NewRecordFromSlice(gb.outputFieldNames, values, opts...))

	newPreviouslyTriggeredValuesTuple := octosql.MakeTuple(values)
	err = previouslyTriggeredValues.Set(&newPreviouslyTriggeredValuesTuple)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't set new previously triggered value")
	}

	return output, nil
}

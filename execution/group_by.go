package execution

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

type AggregatePrototype func() Aggregate

type Aggregate interface {
	AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error
	RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error
	GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error)
	String() string
}

type TriggerPrototype func(ctx context.Context, variables octosql.Variables) (Trigger, error)

type GroupBy struct {
	storage             storage.Storage
	source              Node
	sourceStoragePrefix []byte
	key                 []Expression

	fields              []octosql.VariableName
	aggregatePrototypes []AggregatePrototype
	eventTimeField      octosql.VariableName

	as                []octosql.VariableName
	outEventTimeField octosql.VariableName

	triggerPrototype TriggerPrototype
}

func NewGroupBy(storage storage.Storage, source Node, sourceStoragePrefix []byte, key []Expression, fields []octosql.VariableName, aggregatePrototypes []AggregatePrototype, eventTimeField octosql.VariableName, as []octosql.VariableName, outEventTimeField octosql.VariableName, triggerPrototype TriggerPrototype) *GroupBy {
	return &GroupBy{storage: storage, source: source, sourceStoragePrefix: sourceStoragePrefix, key: key, fields: fields, aggregatePrototypes: aggregatePrototypes, eventTimeField: eventTimeField, as: as, outEventTimeField: outEventTimeField, triggerPrototype: triggerPrototype}
}

func (node *GroupBy) Get(ctx context.Context, variables octosql.Variables) (RecordStream, error) {
	source, err := node.source.Get(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream for source in group by")
	}

	aggregates := make([]Aggregate, len(node.aggregatePrototypes))
	for i := range node.aggregatePrototypes {
		aggregates[i] = node.aggregatePrototypes[i]()
	}
	prefixes := make([][]byte, len(aggregates))
	for i := range prefixes {
		prefixes[i] = []byte(fmt.Sprintf("$agg%d$", i))
	}

	outputFieldNames := make([]octosql.VariableName, len(aggregates))
	for i := range aggregates {
		if len(node.as[i]) > 0 {
			outputFieldNames[i] = node.as[i]
		} else {
			outputFieldNames[i] = octosql.NewVariableName(
				fmt.Sprintf(
					"%s_%s",
					node.fields[i].String(),
					aggregates[i].String(),
				),
			)
		}
	}

	trigger, err := node.triggerPrototype(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get trigger from trigger prototype")
	}

	groupBy := &GroupByStream{
		prefixes:             prefixes,
		inputFields:          node.fields,
		eventTimeField:       node.eventTimeField,
		outputEventTimeField: node.outEventTimeField,
		aggregates:           aggregates,
		outputFieldNames:     outputFieldNames,
	}
	processFunc := &ProcessByKey{
		stateStorage:    node.storage,
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpression:   node.key,
		processFunction: groupBy,
		variables:       variables,
	}
	groupByPullEngine := NewPullEngine(processFunc, node.storage, source, node.sourceStoragePrefix, &ZeroWatermarkSource{})
	go groupByPullEngine.Run(ctx) // TODO: .Close() should kill this context and the goroutine.

	return groupByPullEngine, nil
}

type GroupByStream struct {
	prefixes [][]byte

	eventTimeField octosql.VariableName // Empty if not grouping by event time
	inputFields    []octosql.VariableName
	aggregates     []Aggregate

	outputFieldNames     []octosql.VariableName
	outputEventTimeField octosql.VariableName
}

var recordCountPrefix = []byte("$record_count$")

func (gb *GroupByStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
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

	// Update aggregates
	for i := range gb.aggregates {
		var value octosql.Value
		if gb.inputFields[i] == "*star*" {
			mapping := make(map[string]octosql.Value, len(record.Fields()))
			for _, field := range record.Fields() {
				mapping[field.Name.String()] = record.Value(field.Name)
			}
			value = octosql.MakeObject(mapping)
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

func (gb *GroupByStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	output := make([]*Record, 0, 2)
	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)

	// Check if we have to handle event time
	var opts []RecordOption
	if len(gb.eventTimeField) > 0 {
		opts = append(opts, WithEventTimeField(gb.outputEventTimeField))
	}

	// Handle previously triggered record
	previouslyTriggeredValues := storage.NewValueState(txByKey.WithPrefix(previouslyTriggeredValuePrefix))
	var previouslyTriggered octosql.Value
	err := previouslyTriggeredValues.Get(&previouslyTriggered)
	if err == nil {
		output = append(output, NewRecordFromSlice(gb.outputFieldNames, previouslyTriggered.AsSlice(), append(opts[:len(opts):len(opts)], WithUndo())...))
	} else if err != storage.ErrNotFound {
		return nil, errors.Wrap(err, "couldn't get previously triggered value for key")
	}

	// Check if record count == retraction count
	recordCountState := storage.NewValueState(txByKey.WithPrefix(recordCountPrefix))
	var recordCountValue octosql.Value
	err = recordCountState.Get(&recordCountValue)
	if err == storage.ErrNotFound {
		recordCountValue = octosql.MakeInt(0)
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get current record count")
	}

	if recordCountValue.AsInt() > 0 {
		// Get new record to trigger
		values := make([]octosql.Value, len(gb.aggregates))
		for i := range gb.aggregates {
			var err error
			values[i], err = gb.aggregates[i].GetValue(ctx, txByKey.WithPrefix(gb.prefixes[i]))
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't get value of aggregate %s with index %d", gb.aggregates[i].String(), i)
			}
		}
		output = append(output, NewRecordFromSlice(gb.outputFieldNames, values, opts...))

		// Save currently triggered record
		newPreviouslyTriggeredValuesTuple := octosql.MakeTuple(values)
		err = previouslyTriggeredValues.Set(&newPreviouslyTriggeredValuesTuple)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't set new previously triggered value")
		}
	} else {
		// Clear previously triggered record, as we're sending a retraction for it now
		err = previouslyTriggeredValues.Clear()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't clear previously triggered value")
		}
	}

	// We can have at most one retraction and one normal record.
	// In case we have both, check if they're not equal, because then they cancel each other out and we send neither.
	if len(output) == 2 {
		firstNoUndo := NewRecordFromRecord(output[0], WithNoUndo())
		if firstNoUndo.Equal(output[1]) {
			return nil, nil
		}
	}

	return output, nil
}

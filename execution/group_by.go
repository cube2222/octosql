package execution

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/aggregate"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

type GroupBy struct {
	storage storage.Storage
	source  Node
	key     []Expression

	fields              []octosql.VariableName
	aggregatePrototypes []aggregate.AggregatePrototype
	eventTimeField      octosql.VariableName

	as                []octosql.VariableName
	outEventTimeField octosql.VariableName

	triggerPrototype  TriggerPrototype
	CreatedByDistinct bool
}

func NewGroupBy(storage storage.Storage, source Node, key []Expression, fields []octosql.VariableName, aggregatePrototypes []aggregate.AggregatePrototype, eventTimeField octosql.VariableName, as []octosql.VariableName, outEventTimeField octosql.VariableName, triggerPrototype TriggerPrototype, createdByDistinct bool) *GroupBy {
	return &GroupBy{storage: storage, source: source, key: key, fields: fields, aggregatePrototypes: aggregatePrototypes, eventTimeField: eventTimeField, as: as, outEventTimeField: outEventTimeField, triggerPrototype: triggerPrototype, CreatedByDistinct: createdByDistinct}
}

func (node *GroupBy) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, execOutput, err := node.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get stream for source in group by")
	}

	aggregates := make([]aggregate.Aggregate, len(node.aggregatePrototypes))
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
			if node.CreatedByDistinct {
				outputFieldNames[i] = node.fields[i]
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
	}

	trigger, err := node.triggerPrototype.Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get trigger from trigger prototype")
	}

	// We need to remember star expressions in GroupByStream to fill in missing aggregates
	starExpressions := make([]*StarExpression, 0)
	for _, expr := range node.key {
		if expr, ok := expr.(*StarExpression); ok {
			starExpressions = append(starExpressions, expr)
		}
	}

	groupBy := &GroupByStream{
		prefixes:             prefixes,
		eventTimeField:       node.eventTimeField,
		inputFields:          node.fields,
		aggregates:           aggregates,
		starExpressions:      starExpressions,
		outputFields:         newGroupByOutputFields(outputFieldNames),
		outputEventTimeField: node.outEventTimeField,
	}

	processFunc := &ProcessByKey{
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpression:   node.key,
		processFunction: groupBy,
		variables:       variables,
	}

	groupByPullEngine := NewPullEngine(processFunc, node.storage, source, streamID, execOutput.WatermarkSource)

	go groupByPullEngine.Run(ctx) // TODO: .Close() should kill this context and the goroutine.

	return groupByPullEngine, NewExecutionOutput(groupByPullEngine), nil // groupByPullEngine now indicates new watermark source
}

type GroupByStream struct {
	prefixes [][]byte

	eventTimeField  octosql.VariableName // Empty if not grouping by event time
	inputFields     []octosql.VariableName
	aggregates      []aggregate.Aggregate
	starExpressions []*StarExpression

	outputFields         *groupByOutputFields
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

	// Create values and aggregates for the record
	// This needs to be done here, because different records might result in different values
	// from star expressions (i.e with different number of fields)

	recordVariables := record.AsVariables()

	fieldCount := len(gb.inputFields)

	values := make([]octosql.Value, fieldCount)

	for i, fieldName := range gb.inputFields {
		value := record.Value(fieldName)
		values[i] = value
	}

	// we don't add the outputFields from gb.inputFields because they are already there
	outputFields := make([]octosql.VariableName, 0)
	for _, expr := range gb.starExpressions { // handle star expressions
		value, err := expr.ExpressionValue(ctx, recordVariables)
		if err != nil {
			return errors.Wrap(err, "couldn't get value of star expression in group by")
		}

		values = append(values, value.AsSlice()...)
		outputFields = append(outputFields, expr.Name(recordVariables)...)
	}

	gb.outputFields.extendByFields(outputFields)

	// Add necessary aggregates
	currentAggregateCount := len(gb.aggregates)
	for i := 0; i < len(values)-currentAggregateCount; i++ {
		gb.prefixes = append(gb.prefixes, []byte(fmt.Sprintf("$agg_%d$", currentAggregateCount+i)))
		gb.aggregates = append(gb.aggregates, &aggregate.First{})
	}

	// Update aggregates once we have full info
	for i := range gb.aggregates {
		value := values[i]

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
		output = append(output, NewRecordFromSlice(gb.outputFields.outputFields, previouslyTriggered.AsSlice(), append(opts[:len(opts):len(opts)], WithUndo())...))
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
		output = append(output, NewRecordFromSlice(gb.outputFields.outputFields, values, opts...))

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

type groupByOutputFields struct {
	countMap     map[octosql.VariableName]int
	outputFields []octosql.VariableName
}

func newGroupByOutputFields(initialFields []octosql.VariableName) *groupByOutputFields {
	f := &groupByOutputFields{
		countMap:     make(map[octosql.VariableName]int),
		outputFields: make([]octosql.VariableName, 0),
	}

	for _, field := range initialFields {
		f.addField(field)
	}

	return f
}

func (f *groupByOutputFields) getCount(field octosql.VariableName) int {
	count, ok := f.countMap[field]
	if !ok {
		return 0
	}

	return count
}

func (f *groupByOutputFields) addField(field octosql.VariableName) {
	f.outputFields = append(f.outputFields, field)
	f.countMap[field] = f.getCount(field) + 1
}

func (f *groupByOutputFields) extendByFields(fields []octosql.VariableName) {
	fieldsMapped := newGroupByOutputFields(fields)

	// This isn't very optimal, but it keeps the order intact
	for _, field := range fields {
		fieldCountAlready := f.getCount(field)
		fieldCount := fieldsMapped.getCount(field)

		if fieldCount > fieldCountAlready {
			f.addField(field)
		}
	}
}

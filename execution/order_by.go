package execution

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"

	"github.com/pkg/errors"
)

type OrderDirection string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

type OrderByKey struct {
	key []byte
}

func NewOrderByKey(key []byte) *OrderByKey {
	return &OrderByKey{key: key}
}

func (k *OrderByKey) MonotonicMarshal() []byte {
	return k.key
}

func (k *OrderByKey) MonotonicUnmarshal(data []byte) error {
	k.key = make([]byte, len(data))
	copy(k.key, data)
	return nil
}

type OrderBy struct {
	storage          storage.Storage
	eventTimeField   octosql.VariableName
	expressions      []Expression
	directions       []OrderDirection
	key              []Expression
	source           Node
	triggerPrototype TriggerPrototype
}

func NewOrderBy(storage storage.Storage, source Node, exprs []Expression, directions []OrderDirection, eventTimeField octosql.VariableName, triggerPrototype TriggerPrototype) *OrderBy {
	return &OrderBy{storage: storage, source: source, expressions: exprs, directions: directions, eventTimeField: eventTimeField, triggerPrototype: triggerPrototype}
}

func (node *OrderBy) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, execOutput, err := node.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get stream for source in order by")
	}

	trigger, err := node.triggerPrototype.Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get trigger from trigger prototype")
	}

	node.key = make([]Expression, 0)
	if len(node.eventTimeField) > 0 {
		node.key = append(node.key, node.expressions[0])
	} else {
		node.key = append(node.key, NewConstantValue(octosql.MakeString("key")))
	}

	orderBy := &OrderByStream{
		variables:   variables,
		expressions: node.expressions,
		directions:  node.directions,
	}

	processFunc := &ProcessByKey{
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpressions:  [][]Expression{node.key},
		processFunction: orderBy,
		variables:       variables,
	}

	orderByPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{source}, streamID, execOutput.WatermarkSource, true, ctx)

	return orderByPullEngine, // groupByPullEngine now indicates new watermark source
		NewExecutionOutput(
			orderByPullEngine,
			execOutput.NextShuffles,
			append(execOutput.TasksToRun, func() error { orderByPullEngine.Run(); return nil }),
		),
		nil
}

type OrderByStream struct {
	expressions []Expression
	directions  []OrderDirection
	variables   octosql.Variables
}

var recordValuePrefix = []byte("$record_value$")

//recordCountPrefix defined in group_by

func (ob *OrderByStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	if inputIndex > 0 {
		panic("only one input stream allowed for order by")
	}

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)
	recordValueMap := storage.NewMap(txByKey.WithPrefix(recordValuePrefix))

	mapping := make(map[string]octosql.Value, len(record.Fields()))
	for _, field := range record.Fields() {
		mapping[field.Name.String()] = record.Value(field.Name)
	}
	variables := record.AsVariables()

	recordPrefix := []byte("$")

	for i := range ob.expressions {
		expressionValue, err := ob.expressions[i].ExpressionValue(ctx, variables)
		if err != nil {
			return errors.Wrapf(err, "couldn't evaluate expression with index %d", i)
		}
		if ob.directions[i] == Ascending {
			recordPrefix = append(recordPrefix, expressionValue.MonotonicMarshal()...)
		} else {
			recordPrefix = append(recordPrefix, expressionValue.ReversedMonotonicMarshal()...)
		}
		recordPrefix = append(recordPrefix, '$')
	}

	//append whole record to prefix to avoid errors caused by two different records with the same key
	bytePref := append(append(recordPrefix, []byte(NewRecordFromRecord(record, WithNoUndo()).String())...), '$')
	pref := OrderByKey{key: bytePref}

	recordCountState := storage.NewValueState(txByKey.WithPrefix(recordCountPrefix).WithPrefix(bytePref))
	var recordCount octosql.Value
	err := recordCountState.Get(&recordCount)

	if !record.IsUndo() {
		if err == storage.ErrNotFound {
			err = recordValueMap.Set(&pref, record)
			if err != nil {
				return errors.Wrap(err, "couldn't add record")
			}

			recordCount = octosql.MakeInt(1)
			err = recordCountState.Set(&recordCount)
			if err != nil {
				return errors.Wrap(err, "couldn't update record count")
			}
		} else if err != nil {
			return errors.Wrap(err, "couldn't get record count")
		} else {
			// Keep track of record vs retraction count
			recordCount = octosql.MakeInt(recordCount.AsInt() + 1)
			err = recordCountState.Set(&recordCount)
			if err != nil {
				return errors.Wrap(err, "couldn't update record count")
			}
		}
	} else {
		if err == storage.ErrNotFound {
			err = recordValueMap.Set(&pref, record)
			if err != nil {
				return errors.Wrap(err, "couldn't add record")
			}

			recordCount = octosql.MakeInt(-1)
			err = recordCountState.Set(&recordCount)
			if err != nil {
				return errors.Wrap(err, "couldn't update record count")
			}
		} else if err != nil {
			return errors.Wrap(err, "couldn't get record count")
		} else {
			// Keep track of record vs retraction count
			recordCount = octosql.MakeInt(recordCount.AsInt() - 1)
			err = recordCountState.Set(&recordCount)
			if err != nil {
				return errors.Wrap(err, "couldn't update record count")
			}
		}
	}

	return nil
}

func (ob *OrderByStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	output := make([]*Record, 0)

	keyPrefix := append(append([]byte("$"), key.MonotonicMarshal()...), '$')
	txByKey := tx.WithPrefix(keyPrefix)
	recordValueMap := storage.NewMap(txByKey.WithPrefix(recordValuePrefix))

	pref := OrderByKey{key: []byte("")}

	var count octosql.Value
	var value Record
	var err error
	iter := recordValueMap.GetIterator()
	for err = iter.Next(&pref, &value); err == nil; err = iter.Next(&pref, &value) {
		recordCountState := storage.NewValueState(txByKey.WithPrefix(recordCountPrefix).WithPrefix(pref.key))
		err := recordCountState.Get(&count)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get record count")
		}

		for i := 0; i < count.AsInt(); i++ {
			newValue := value
			output = append(output, &newValue)
		}
	}

	if err != storage.ErrEndOfIterator {
		return nil, errors.Wrap(err, "couldn't iterate over existing records")
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "couldn't close record iterator")
	}

	return output, nil
}

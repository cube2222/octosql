package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type AggregatePrototype func() Aggregate

type Aggregate interface {
	AddRecord(key []interface{}, value interface{}) error
	GetAggregated(key []Expression) (interface{}, error)
}

type GroupBy struct {
	source Node
	key    []Expression

	fields              []octosql.VariableName
	aggregatePrototypes []AggregatePrototype
}

func NewGroupBy(source Node, key []Expression, fields []octosql.VariableName, aggregatePrototypes []AggregatePrototype) *GroupBy {
	return &GroupBy{source: source, key: key, fields: fields, aggregatePrototypes: aggregatePrototypes}
}

func (node *GroupBy) Get(variables octosql.Variables) (RecordStream, error) {
	source, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream for source in group by")
	}

	aggregates := make([]Aggregate, len(node.aggregatePrototypes))
	for i := range node.aggregatePrototypes {
		aggregates[i] = node.aggregatePrototypes[i]()
	}

	return &GroupByStream{
		source:     source,
		fields:     node.fields,
		aggregates: aggregates,
	}, nil
}

type GroupByStream struct {
	source     RecordStream
	fields     []octosql.VariableName
	aggregates []Aggregate
	set        recordSet
}

func (node *GroupByStream) Next() (*Record, error) {
	record, err := node.source.Next()
	if err != nil {
		// TODO: ErrEndOfStream
		return nil, errors.Wrap(err, "couldn't get next source record")
	}

	values := make([]interface{}, len(node.fields))
	for i := range node.fields {
		values[i] = record.Value(node.fields[i])
	}
	_, err = node.set.Insert(NewRecordFromSlice(node.fields, values))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't insert record into set")
	}

	for i := range node.aggregates {
		err := node.aggregates[i].AddRecord(values, record.Value(node.fields[i]))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't add record to aggregate with index %v", i)
		}
	}

	return nil, ErrEndOfStream
}

func (node *GroupByStream) Close() error {
	return node.source.Close()
}

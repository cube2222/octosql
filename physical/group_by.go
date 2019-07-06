package physical

import (
	"context"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/aggregates"
	"github.com/pkg/errors"
)

type Aggregate string

const (
	Avg   Aggregate = "avg"
	Count Aggregate = "count"
	First Aggregate = "first"
	Last  Aggregate = "last"
	Max   Aggregate = "max"
	Min   Aggregate = "min"
	Sum   Aggregate = "sum"
)

var aggregateTable = map[Aggregate]execution.AggregatePrototype{ // TODO: fillme
	Count: func() execution.Aggregate { return aggregates.NewCount() },
	First: func() execution.Aggregate { return aggregates.NewFirst() },
}

func NewAggregate(aggregate string) Aggregate {
	return Aggregate(strings.ToLower(aggregate))
}

type GroupBy struct {
	Source Node
	Key    []Expression

	Fields     []octosql.VariableName
	Aggregates []Aggregate
}

func NewGroupBy(source Node, key []Expression, fields []octosql.VariableName, aggregates []Aggregate) *GroupBy {
	return &GroupBy{Source: source, Key: key, Fields: fields, Aggregates: aggregates}
}

func (node *GroupBy) Transform(ctx context.Context, transformers *Transformers) Node {
	key := make([]Expression, len(node.Key))
	for i := range node.Key {
		key[i] = node.Key[i].Transform(ctx, transformers)
	}

	source := node.Source.Transform(ctx, transformers)

	var transformed Node = &GroupBy{
		Source:     source,
		Key:        key,
		Fields:     node.Fields,
		Aggregates: node.Aggregates,
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}

	return transformed
}

func (node *GroupBy) Materialize(ctx context.Context) (execution.Node, error) {
	source, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source node")
	}

	key := make([]execution.Expression, len(node.Key))
	for i := range node.Key {
		keyPart, err := node.Key[i].Materialize(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize group key expression with index %v", i)
		}

		key[i] = keyPart
	}

	aggregatePrototypes := make([]execution.AggregatePrototype, len(node.Aggregates))
	for i := range node.Aggregates {
		aggregatePrototypes[i] = aggregateTable[node.Aggregates[i]]
	}

	return execution.NewGroupBy(source, key, node.Fields, aggregatePrototypes), nil
}

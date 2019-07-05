package logical

import (
	"context"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
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

type GroupBy struct {
	source Node
	key    []Expression

	fields     []octosql.VariableName
	aggregates []Aggregate
}

func NewGroupBy(source Node, key []Expression, fields []octosql.VariableName, aggregates []Aggregate) *GroupBy {
	return &GroupBy{source: source, key: key, fields: fields, aggregates: aggregates}
}

func (node *GroupBy) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	source, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for group by source")
	}
	variables, err = variables.MergeWith(sourceVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables with those of source")
	}

	key := make([]physical.Expression, len(node.key))
	for i := range node.key {
		expr, exprVariables, err := node.key[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for group key expression with index %d", i)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of group key expression with index %d", i)
		}

		key[i] = expr
	}

	aggregates := make([]physical.Aggregate, len(node.aggregates))
	for i := range node.aggregates {
		switch Aggregate(strings.ToLower(string(node.aggregates[i]))) {
		case Avg:
			aggregates[i] = physical.Avg
		case Count:
			aggregates[i] = physical.Count
		case First:
			aggregates[i] = physical.First
		case Last:
			aggregates[i] = physical.Last
		case Max:
			aggregates[i] = physical.Max
		case Min:
			aggregates[i] = physical.Min
		case Sum:
			aggregates[i] = physical.Sum
		default:
			return nil, nil, errors.Errorf("invalid aggregate: %s", node.aggregates[i])
		}
	}

	return physical.NewGroupBy(source, key, node.fields, aggregates), variables, nil
}

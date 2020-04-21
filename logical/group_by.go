package logical

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Aggregate string

const (
	Avg           Aggregate = "avg"
	AvgDistinct   Aggregate = "avg_distinct"
	Count         Aggregate = "count"
	CountDistinct Aggregate = "count_distinct"
	First         Aggregate = "first"
	Last          Aggregate = "last"
	Max           Aggregate = "max"
	Min           Aggregate = "min"
	Sum           Aggregate = "sum"
	SumDistinct   Aggregate = "sum_distinct"
)

var AggregateFunctions = map[Aggregate]struct{}{
	Avg:           struct{}{},
	AvgDistinct:   struct{}{},
	Count:         struct{}{},
	CountDistinct: struct{}{},
	First:         struct{}{},
	Last:          struct{}{},
	Max:           struct{}{},
	Min:           struct{}{},
	Sum:           struct{}{},
	SumDistinct:   struct{}{},
}

type GroupBy struct {
	source Node
	key    []Expression

	fields     []octosql.VariableName
	aggregates []Aggregate

	as []octosql.VariableName
}

func (groupBy *GroupBy) Visualize() *graph.Node {
	n := graph.NewNode("Group By")
	if groupBy.source != nil {
		n.AddChild("source", groupBy.source.Visualize())
	}

	for idx := range groupBy.key {
		n.AddChild("key_"+strconv.Itoa(idx), groupBy.key[idx].Visualize())
	}

	for i := range groupBy.fields {
		value := fmt.Sprintf("%s(%s)", groupBy.aggregates[i], groupBy.fields[i])
		if i < len(groupBy.as) && !groupBy.as[i].Empty() {
			value += fmt.Sprintf(" as %s", groupBy.as[i])
		}
		n.AddField(fmt.Sprintf("field_%d", i), value)
	}
	return n
}

func NewGroupBy(source Node, key []Expression, fields []octosql.VariableName, aggregates []Aggregate, as []octosql.VariableName) *GroupBy {
	return &GroupBy{source: source, key: key, fields: fields, aggregates: aggregates, as: as}
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
		case AvgDistinct:
			aggregates[i] = physical.AvgDistinct
		case Count:
			aggregates[i] = physical.Count
		case CountDistinct:
			aggregates[i] = physical.CountDistinct
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
		case SumDistinct:
			aggregates[i] = physical.SumDistinct
		default:
			return nil, nil, errors.Errorf("invalid aggregate: %s", node.aggregates[i])
		}
	}

	return physical.NewGroupBy(source, key, node.fields, aggregates, node.as), variables, nil
}

package physical

import (
	"context"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/pkg/errors"
	"strings"
)

type Aggregate string

const (
	Count Aggregate = "count"
	Max   Aggregate = "max"
	Min   Aggregate = "min"
	Avg   Aggregate = "avg"
)

func NewAggregate(aggr logical.Aggregate) Aggregate {
	return Aggregate(aggr)
}

type GroupBy struct {
	SelectExpr []NamedExpression
	Source     Node
	Group      []NamedExpression             // po czym grupujemy
	Aggregates map[NamedExpression]Aggregate // zbior agregatow: agregat -> co agreguje
}

func NewGroupBy(selectExpr []NamedExpression, source Node, group []NamedExpression, aggregates map[NamedExpression]Aggregate) *GroupBy {
	return &GroupBy{SelectExpr: selectExpr, Source: source, Group: group, Aggregates: aggregates}
}

func (node *GroupBy) Transform(ctx context.Context, transformers *Transformers) Node {
	// selectExpr transformation
	executionSelect := make([]NamedExpression, len(node.SelectExpr))
	for i := range node.SelectExpr {
		executionSelect[i] = node.SelectExpr[i].TransformNamed(ctx, transformers)
	}

	// group transformation
	executionGroup := make([]NamedExpression, len(node.Group))
	for i := range node.Group {
		executionGroup[i] = node.Group[i].TransformNamed(ctx, transformers)
	}

	// aggregates transformation
	executionAggregates := make(map[NamedExpression]Aggregate)
	for key, value := range node.Aggregates {
		executionAggregates[key.TransformNamed(ctx, transformers)] = value
	}

	var transformed Node = &GroupBy{
		SelectExpr: executionSelect,
		Source:     node.Source.Transform(ctx, transformers),
		Group:      executionGroup,
		Aggregates: executionAggregates,
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}

	return transformed
}

func (node *GroupBy) Materialize(ctx context.Context) (execution.Node, error) {
	// select materialization
	executionSelect := make([]execution.NamedExpression, len(node.SelectExpr))
	for i := range node.SelectExpr {
		materialized, err := node.SelectExpr[i].MaterializeNamed(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}

		executionSelect[i] = materialized
	}

	// source materialization
	executionSource, err := node.Source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source node")
	}

	// group materialization
	executionGroup := make([]execution.NamedExpression, len(node.Group))
	for i := range node.Group {
		materialized, err := node.Group[i].MaterializeNamed(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}

		executionGroup[i] = materialized
	}

	// aggregates materialization
	executionAggregates := make(map[execution.NamedExpression]execution.Aggregate)
	for key, value := range node.Aggregates {
		executionExpr, err := key.MaterializeNamed(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get execution plan for aggregates map with key %s", key)
		}

		switch Aggregate(strings.ToLower(string(value))) {
		case Count:
			executionAggregates[executionExpr] = execution.NewCount()
		case Max:
			executionAggregates[executionExpr] = execution.NewMax()
		case Min:
			executionAggregates[executionExpr] = execution.NewMin()
		case Avg:
			executionAggregates[executionExpr] = execution.NewAvg()
		default:
			return nil, errors.Errorf("invalid aggregate %s", key)
		}
	}

	return execution.NewGroupBy(executionSelect, executionSource, executionGroup, executionAggregates), nil
}

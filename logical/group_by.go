package logical

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Aggregate string

const (
	Count Aggregate = "count"
	Max   Aggregate = "max"
	Min   Aggregate = "min"
	Avg   Aggregate = "avg"
)

type GroupBy struct {
	selectExpr []NamedExpression
	source     Node
	group      []NamedExpression             // po czym grupujemy
	aggregates map[NamedExpression]Aggregate // zbior agregatow: agregat -> co agreguje
}

func NewGroupBy(selectExpr []NamedExpression, source Node, group []NamedExpression, aggregates map[NamedExpression]Aggregate) *GroupBy {
	return &GroupBy{selectExpr: selectExpr, source: source, group: group, aggregates: aggregates}
}

func (node *GroupBy) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	// selectexpr convertion
	physicalSelect := make([]physical.NamedExpression, len(node.selectExpr))
	for i := range node.selectExpr {
		physicalExpr, exprVariables, err := node.selectExpr[i].PhysicalNamed(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for group expression with index %d", i)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of group expression with index %d", i)
		}

		physicalSelect[i] = physicalExpr
	}

	// source convertion
	physicalSource, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for group by source")
	}
	variables, err = variables.MergeWith(sourceVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables with those of source")
	}

	// group convertion
	physicalGroup := make([]physical.NamedExpression, len(node.group))
	for i := range node.group {
		physicalExpr, exprVariables, err := node.group[i].PhysicalNamed(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for group expression with index %d", i)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of group expression with index %d", i)
		}

		physicalGroup[i] = physicalExpr
	}

	// aggregates convertion
	physicalAggregates := make(map[physical.NamedExpression]physical.Aggregate)
	for key, value := range node.aggregates {
		physicalExpr, exprVariables, err := key.PhysicalNamed(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for aggregates map with key %s", key)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of aggregates expression with key %s", key)
		}

		physicalAggregates[physicalExpr] = physical.NewAggregate(value)
	}

	return physical.NewGroupBy(physicalSelect, physicalSource, physicalGroup, physicalAggregates), variables, nil
}

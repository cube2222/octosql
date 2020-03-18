package logical

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type Aggregate string

const (
	Avg           Aggregate = "avg"
	AvgDistinct   Aggregate = "avg_distinct"
	Count         Aggregate = "count"
	CountDistinct Aggregate = "count_distinct"
	First         Aggregate = "first"
	Key           Aggregate = "key"
	Last          Aggregate = "last"
	Max           Aggregate = "max"
	Min           Aggregate = "min"
	Sum           Aggregate = "sum"
	SumDistinct   Aggregate = "sum_distinct"
)

var AggregateFunctions = map[Aggregate]struct{}{
	Avg:           {},
	AvgDistinct:   {},
	Count:         {},
	CountDistinct: {},
	First:         {},
	Key:           {},
	Last:          {},
	Max:           {},
	Min:           {},
	Sum:           {},
	SumDistinct:   {},
}

type Trigger interface {
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Trigger, octosql.Variables, error)
}

type CountingTrigger struct {
	Count Expression
}

func NewCountingTrigger(count Expression) *CountingTrigger {
	return &CountingTrigger{Count: count}
}

func (w *CountingTrigger) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Trigger, octosql.Variables, error) {
	countExpr, vars, err := w.Count.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for count expression")
	}
	return physical.NewCountingTrigger(countExpr), vars, nil
}

type DelayTrigger struct {
	Delay Expression
}

func NewDelayTrigger(delay Expression) *DelayTrigger {
	return &DelayTrigger{Delay: delay}
}

func (w *DelayTrigger) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Trigger, octosql.Variables, error) {
	delayExpr, vars, err := w.Delay.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for delay expression")
	}
	return physical.NewDelayTrigger(delayExpr), vars, nil
}

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (w *WatermarkTrigger) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Trigger, octosql.Variables, error) {
	return physical.NewWatermarkTrigger(), octosql.NoVariables(), nil
}

type GroupBy struct {
	source Node
	key    []Expression

	fields     []octosql.VariableName
	aggregates []Aggregate

	as []octosql.VariableName

	triggers          []Trigger
	createdByDistinct bool
}

func NewGroupBy(source Node, key []Expression, fields []octosql.VariableName, aggregates []Aggregate, as []octosql.VariableName, triggers []Trigger, createdByDistinct bool) *GroupBy {
	return &GroupBy{source: source, key: key, fields: fields, aggregates: aggregates, as: as, triggers: triggers, createdByDistinct: createdByDistinct}
}

func (node *GroupBy) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	sourceNodes, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for group by sources")
	}
	variables, err = variables.MergeWith(sourceVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables with those of sources")
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
		case Key:
			aggregates[i] = physical.Key
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

	triggers := make([]physical.Trigger, len(node.triggers))
	for i := range node.triggers {
		out, triggerVariables, err := node.triggers[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for trigger with index %d", i)
		}
		variables, err = variables.MergeWith(triggerVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of trigger with index %d", i)
		}

		triggers[i] = out
	}

	outNodes := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)
	for i := range outNodes {
		outNodes[i] = physical.NewGroupBy(outNodes[i], key, node.fields, aggregates, node.as, triggers, node.createdByDistinct)
	}

	return outNodes, variables, nil
}

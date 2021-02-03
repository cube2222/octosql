package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type Trigger interface {
	Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Trigger, error)
}

type CountingTrigger struct {
	Count Expression
}

func NewCountingTrigger(count Expression) *CountingTrigger {
	return &CountingTrigger{Count: count}
}

func (w *CountingTrigger) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Trigger, error) {
	panic("implement me")
}

type DelayTrigger struct {
	Delay Expression
}

func NewDelayTrigger(delay Expression) *DelayTrigger {
	return &DelayTrigger{Delay: delay}
}

func (w *DelayTrigger) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Trigger, error) {
	panic("implement me")
}

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (w *WatermarkTrigger) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Trigger, error) {
	panic("implement me")
}

type GroupBy struct {
	source Node
	key    []Expression

	fields     []string
	aggregates []string

	as []string

	triggers []Trigger
}

func NewGroupBy(source Node, key []Expression, fields []string, aggregates []string, as []string, triggers []Trigger) *GroupBy {
	return &GroupBy{source: source, key: key, fields: fields, aggregates: aggregates, as: as, triggers: triggers}
}

func (node *GroupBy) Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error) {
	panic("implement me")
	// variables := octosql.NoVariables()
	//
	// sourceNodes, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	// if err != nil {
	// 	return nil, nil, errors.Wrap(err, "couldn't get physical plan for group by sources")
	// }
	// variables, err = variables.MergeWith(sourceVariables)
	// if err != nil {
	// 	return nil, nil, errors.Wrap(err, "couldn't merge variables with those of sources")
	// }
	//
	// key := make([]physical.Expression, len(node.key))
	// for i := range node.key {
	// 	expr, exprVariables, err := node.key[i].Physical(ctx, physicalCreator)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(err, "couldn't get physical plan for group key expression with index %d", i)
	// 	}
	// 	variables, err = variables.MergeWith(exprVariables)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of group key expression with index %d", i)
	// 	}
	//
	// 	key[i] = expr
	// }
	//
	// aggregates := make([]physical.Aggregate, len(node.aggregates))
	// for i := range node.aggregates {
	// 	switch Aggregate(strings.ToLower(string(node.aggregates[i]))) {
	// 	case Avg:
	// 		aggregates[i] = physical.Avg
	// 	case AvgDistinct:
	// 		aggregates[i] = physical.AvgDistinct
	// 	case Count:
	// 		aggregates[i] = physical.Count
	// 	case CountDistinct:
	// 		aggregates[i] = physical.CountDistinct
	// 	case First:
	// 		aggregates[i] = physical.First
	// 	case Key:
	// 		aggregates[i] = physical.Key
	// 	case Last:
	// 		aggregates[i] = physical.Last
	// 	case Max:
	// 		aggregates[i] = physical.Max
	// 	case Min:
	// 		aggregates[i] = physical.Min
	// 	case Sum:
	// 		aggregates[i] = physical.Sum
	// 	case SumDistinct:
	// 		aggregates[i] = physical.SumDistinct
	// 	default:
	// 		return nil, nil, errors.Errorf("invalid aggregate: %s", node.aggregates[i])
	// 	}
	// }
	//
	// triggers := make([]physical.Trigger, len(node.triggers))
	// for i := range node.triggers {
	// 	out, triggerVariables, err := node.triggers[i].Physical(ctx, physicalCreator)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(err, "couldn't get physical plan for trigger with index %d", i)
	// 	}
	// 	variables, err = variables.MergeWith(triggerVariables)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of trigger with index %d", i)
	// 	}
	//
	// 	triggers[i] = out
	// }
	//
	// groupByParallelism, err := config.GetInt(
	// 	physicalCreator.physicalConfig,
	// 	"groupByParallelism",
	// 	config.WithDefault(runtime.GOMAXPROCS(0)),
	// )
	// if err != nil {
	// 	return nil, nil, errors.Wrap(err, "couldn't get groupByParallelism configuration")
	// }
	//
	// outNodes := physical.NewShuffle(groupByParallelism, physical.NewKeyHashingStrategy(key), sourceNodes)
	// for i := range outNodes {
	// 	outNodes[i] = physical.NewGroupBy(outNodes[i], key, node.fields, aggregates, node.as, triggers)
	// }
	//
	// return outNodes, variables, nil
}

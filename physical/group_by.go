package physical

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/trigger"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/streaming/aggregate"
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

func NewAggregate(aggregate string) Aggregate {
	return Aggregate(strings.ToLower(aggregate))
}

type Trigger interface {
	// Transform returns a new Expression after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Trigger
	Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.TriggerPrototype, error)
}

type CountingTrigger struct {
	Count Expression
}

func NewCountingTrigger(count Expression) *CountingTrigger {
	return &CountingTrigger{Count: count}
}

func (c *CountingTrigger) Transform(ctx context.Context, transformers *Transformers) Trigger {
	var transformed Trigger = &CountingTrigger{
		Count: c.Count.Transform(ctx, transformers),
	}
	if transformers.TriggerT != nil {
		transformed = transformers.TriggerT(transformed)
	}
	return transformed
}

func (c *CountingTrigger) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.TriggerPrototype, error) {
	countExpr, err := c.Count.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize count expression")
	}
	return func(ctx context.Context, variables octosql.Variables) (execution.Trigger, error) {
		count, err := countExpr.ExpressionValue(ctx, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get count expression value")
		}
		if t := count.GetType(); t != octosql.TypeInt {
			return nil, errors.Errorf("counting trigger argument must be int, got %v", t)
		}
		return trigger.NewCountingTrigger(count.AsInt()), nil
	}, nil
}

type DelayTrigger struct {
	Delay Expression
}

func NewDelayTrigger(delay Expression) *DelayTrigger {
	return &DelayTrigger{Delay: delay}
}

func (c *DelayTrigger) Transform(ctx context.Context, transformers *Transformers) Trigger {
	var transformed Trigger = &DelayTrigger{
		Delay: c.Delay.Transform(ctx, transformers),
	}
	if transformers.TriggerT != nil {
		transformed = transformers.TriggerT(transformed)
	}
	return transformed
}

func (c *DelayTrigger) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.TriggerPrototype, error) {
	delayExpr, err := c.Delay.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize delay expression")
	}
	return func(ctx context.Context, variables octosql.Variables) (execution.Trigger, error) {
		delay, err := delayExpr.ExpressionValue(ctx, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get delay expression value")
		}
		if t := delay.GetType(); t != octosql.TypeDuration {
			return nil, errors.Errorf("delay trigger argument must be duration, got %v", t)
		}
		return trigger.NewDelayTrigger(delay.AsDuration(), func() time.Time {
			return time.Now()
		}), nil
	}, nil
}

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (c *WatermarkTrigger) Transform(ctx context.Context, transformers *Transformers) Trigger {
	var transformed Trigger = &WatermarkTrigger{}
	if transformers.TriggerT != nil {
		transformed = transformers.TriggerT(transformed)
	}
	return transformed
}

func (c *WatermarkTrigger) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.TriggerPrototype, error) {
	return func(ctx context.Context, variables octosql.Variables) (execution.Trigger, error) {
		return trigger.NewWatermarkTrigger(), nil
	}, nil
}

type GroupBy struct {
	Source Node
	Key    []Expression

	Fields     []octosql.VariableName
	Aggregates []Aggregate

	As []octosql.VariableName

	Triggers []Trigger
}

func NewGroupBy(source Node, key []Expression, fields []octosql.VariableName, aggregates []Aggregate, as []octosql.VariableName, triggers []Trigger) *GroupBy {
	return &GroupBy{Source: source, Key: key, Fields: fields, Aggregates: aggregates, As: as, Triggers: triggers}
}

func (node *GroupBy) Transform(ctx context.Context, transformers *Transformers) Node {
	key := make([]Expression, len(node.Key))
	for i := range node.Key {
		key[i] = node.Key[i].Transform(ctx, transformers)
	}

	source := node.Source.Transform(ctx, transformers)

	triggers := make([]Trigger, len(node.Triggers))
	for i := range node.Triggers {
		triggers[i] = node.Triggers[i].Transform(ctx, transformers)
	}

	var transformed Node = &GroupBy{
		Source:     source,
		Key:        key,
		Fields:     node.Fields,
		Aggregates: node.Aggregates,
		As:         node.As,
		Triggers:   triggers,
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}

	return transformed
}

func (node *GroupBy) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	sourceMatCtx, sourceStoragePrefix := matCtx.WithStoragePrefix()

	source, err := node.Source.Materialize(ctx, sourceMatCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize Source node")
	}

	key := make([]execution.Expression, len(node.Key))
	for i := range node.Key {
		keyPart, err := node.Key[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize group key expression with index %v", i)
		}

		key[i] = keyPart
	}

	aggregatePrototypes := make([]execution.AggregatePrototype, len(node.Aggregates))
	for i := range node.Aggregates {
		aggregatePrototypes[i] = aggregate.AggregateTable[string(node.Aggregates[i])]
	}

	triggerPrototypes := make([]execution.TriggerPrototype, len(node.Triggers))
	for i := range node.Triggers {
		triggerPrototypes[i], err = node.Triggers[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize trigger with index %v", i)
		}
	}

	triggerPrototype := func(ctx context.Context, variables octosql.Variables) (execution.Trigger, error) {
		if len(triggerPrototypes) == 0 {
			return trigger.NewWatermarkTrigger(), nil
		}

		triggers := make([]execution.Trigger, len(triggerPrototypes))
		for i := range triggerPrototypes {
			out, err := triggerPrototypes[i](ctx, variables)
			if err != nil {
				return nil, errors.Wrapf(err, "couldnt get trigger from trigger prototype with index %d", i)
			}

			triggers[i] = out
		}

		return trigger.NewMultiTrigger(triggers...), nil
	}

	sourceMetadata := node.Source.Metadata()
	eventTimeField := octosql.NewVariableName("")
	if node.groupingByEventTime(sourceMetadata) {
		eventTimeField = sourceMetadata.EventTimeField()
	}

	meta := node.Metadata()

	return execution.NewGroupBy(matCtx.Storage, source, sourceStoragePrefix, key, node.Fields, aggregatePrototypes, eventTimeField, node.As, meta.EventTimeField(), triggerPrototype), nil
}

func (node *GroupBy) groupingByEventTime(sourceMetadata *metadata.NodeMetadata) bool {
	groupingByEventTime := false
	if !sourceMetadata.EventTimeField().Empty() {
		for i := range node.Key {
			if variable, ok := node.Key[i].(*Variable); ok {
				if variable.Name == sourceMetadata.EventTimeField() {
					groupingByEventTime = true
				}
			}
		}
	}

	return groupingByEventTime
}

func (node *GroupBy) Metadata() *metadata.NodeMetadata {
	sourceMetadata := node.Source.Metadata()
	var cardinality = sourceMetadata.Cardinality()
	if cardinality == metadata.BoundedDoesntFitInLocalStorage {
		cardinality = metadata.BoundedFitsInLocalStorage
	}

	groupingByEventTime := node.groupingByEventTime(sourceMetadata)

	outEventTimeField := octosql.NewVariableName("")
	if groupingByEventTime {
		for i := range node.Fields {
			if node.Aggregates[i] == Key && node.Fields[i] == sourceMetadata.EventTimeField() {
				outEventTimeField = node.As[i]
			}
		}
	}

	return metadata.NewNodeMetadata(cardinality, outEventTimeField)
}

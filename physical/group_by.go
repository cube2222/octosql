package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/aggregates"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
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
	graph.Visualizer
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
	return execution.NewCountingTrigger(countExpr), nil
}

func (c *CountingTrigger) Visualize() *graph.Node {
	n := graph.NewNode("Counting Trigger")
	n.AddChild("count", c.Count.Visualize())
	return n
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
	return execution.NewDelayTrigger(delayExpr), nil
}

func (c *DelayTrigger) Visualize() *graph.Node {
	n := graph.NewNode("Delay Trigger")
	n.AddChild("count", c.Delay.Visualize())
	return n
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
	return execution.NewWatermarkTrigger(), nil
}

func (c *WatermarkTrigger) Visualize() *graph.Node {
	n := graph.NewNode("Watermark Trigger")
	return n
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
	source, err := node.Source.Materialize(ctx, matCtx)
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
		aggregatePrototypes[i] = aggregates.AggregateTable[string(node.Aggregates[i])]
	}

	triggerPrototypes := make([]execution.TriggerPrototype, len(node.Triggers))
	for i := range node.Triggers {
		triggerPrototypes[i], err = node.Triggers[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize trigger with index %v", i)
		}
	}

	var triggerPrototype execution.TriggerPrototype
	if len(triggerPrototypes) == 0 {
		triggerPrototype = execution.NewWatermarkTrigger()
	} else {
		triggerPrototype = execution.NewMultiTrigger(triggerPrototypes...)
	}

	sourceMetadata := node.Source.Metadata()
	eventTimeField := octosql.NewVariableName("")
	if node.groupingByEventTime(sourceMetadata) {
		eventTimeField = sourceMetadata.EventTimeField()
	}

	meta := node.Metadata()

	return execution.NewGroupBy(matCtx.Storage, source, key, node.Fields, aggregatePrototypes, eventTimeField, node.As, meta.EventTimeField(), triggerPrototype), nil
}

func (node *GroupBy) groupingByEventTime(sourceMetadata *metadata.NodeMetadata) bool {
	if !sourceMetadata.EventTimeField().Empty() {
		for i := range node.Key {
			if variable, ok := node.Key[i].(*Variable); ok {
				if variable.ExpressionName() == sourceMetadata.EventTimeField() {
					return true
				}
			}
		}
	}

	return false
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

	return metadata.NewNodeMetadata(cardinality, outEventTimeField, metadata.EmptyNamespace())
}

func (node *GroupBy) Visualize() *graph.Node {
	n := graph.NewNode("Group By")

	n.AddChild("source", node.Source.Visualize())
	for i, expr := range node.Key {
		n.AddChild(fmt.Sprintf("key_%d", i), expr.Visualize())
	}
	for i, trigger := range node.Triggers {
		n.AddChild(fmt.Sprintf("trigger_%d", i), trigger.Visualize())
	}

	for i := range node.Fields {
		value := fmt.Sprintf("%s(%s)", node.Aggregates[i], node.Fields[i])
		if !node.As[i].Empty() {
			value += fmt.Sprintf(" as %s", node.As[i])
		}
		n.AddField(fmt.Sprintf("field_%d", i), value)
	}

	eventTimeField := node.Metadata().EventTimeField()
	if eventTimeField != "" {
		n.AddField("event_time_field", eventTimeField.String())
	}
	return n
}

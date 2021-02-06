package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Trigger interface {
	Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Trigger
}

type CountingTrigger struct {
	Count Expression
}

func NewCountingTrigger(count Expression) *CountingTrigger {
	return &CountingTrigger{Count: count}
}

func (w *CountingTrigger) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Trigger {
	panic("implement me")
}

type DelayTrigger struct {
	Delay Expression
}

func NewDelayTrigger(delay Expression) *DelayTrigger {
	return &DelayTrigger{Delay: delay}
}

func (w *DelayTrigger) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Trigger {
	panic("implement me")
}

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (w *WatermarkTrigger) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Trigger {
	panic("implement me")
}

type GroupBy struct {
	source Node
	key    []Expression

	expressions []Expression
	aggregates  []string

	triggers []Trigger
}

func NewGroupBy(source Node, key []Expression, expressions []Expression, aggregates []string, triggers []Trigger) *GroupBy {
	return &GroupBy{source: source, key: key, expressions: expressions, aggregates: aggregates, triggers: triggers}
}

func (node *GroupBy) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Node {
	source := node.source.Typecheck(ctx, env, state)

	// TODO: Event time has to be the first defined element of the key.
	key := make([]physical.Expression, len(node.key))
	for i := range node.key {
		key[i] = node.key[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), state)
	}

	expressions := make([]physical.Expression, len(node.expressions))
	for i := range node.expressions {
		expressions[i] = node.expressions[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), state)
	}

	aggregates := make([]physical.Aggregate, len(node.aggregates))
aggregateLoop:
	for i, aggname := range node.aggregates {
		descriptors := env.Aggregates[aggname]
		for _, descriptor := range descriptors {
			if expressions[i].Type.Is(descriptor.ArgumentType) == octosql.TypeRelationIs {
				aggregates[i] = physical.Aggregate{
					Name:       node.aggregates[i],
					OutputType: descriptor.OutputType,
				}
				continue aggregateLoop
			}
		}
		for _, descriptor := range descriptors {
			if expressions[i].Type.Is(descriptor.ArgumentType) == octosql.TypeRelationMaybe {
				aggregates[i] = physical.Aggregate{
					Name:       node.aggregates[i],
					OutputType: descriptor.OutputType,
				}
				expressions[i] = physical.Expression{
					ExpressionType: physical.ExpressionTypeTypeAssertion,
					Type:           descriptor.ArgumentType,
					TypeAssertion: &physical.TypeAssertion{
						Expression: expressions[i],
						TargetType: descriptor.ArgumentType,
					},
				}
				continue aggregateLoop
			}
		}
		panic(fmt.Sprintf("no such aggregate: %s(%s)", aggname, expressions[i].Type))
	}

	triggers := make([]physical.Trigger, len(node.triggers))
	for i := range node.triggers {
		triggers[i] = node.triggers[i].Typecheck(ctx, env, state)
	}
	var trigger physical.Trigger
	if len(triggers) == 0 {
		trigger = physical.Trigger{
			TriggerType:        physical.TriggerTypeEndOfStream,
			EndOfStreamTrigger: &physical.EndOfStreamTrigger{},
		}
	} else if len(triggers) == 1 {
		trigger = triggers[0]
	} else {
		trigger = physical.Trigger{
			TriggerType: physical.TriggerTypeMulti,
			MultiTrigger: &physical.MultiTrigger{
				Triggers: triggers,
			},
		}
	}

	schemaFields := make([]physical.SchemaField, len(key)+len(aggregates))
	for i := range key {
		schemaFields[i] = physical.SchemaField{
			Name: fmt.Sprintf("key_%d", i),
			Type: key[i].Type,
		}
	}
	for i := range aggregates {
		schemaFields[len(key)+i] = physical.SchemaField{
			Name: fmt.Sprintf("agg_%d", i),
			Type: aggregates[i].OutputType,
		}
	}

	// TODO: Calculate time field if grouping by time field.

	return physical.Node{
		Schema:   physical.NewSchema(schemaFields, -1),
		NodeType: physical.NodeTypeGroupBy,
		GroupBy: &physical.GroupBy{
			Source:               source,
			Aggregates:           aggregates,
			AggregateExpressions: expressions,
			Key:                  key,
			Trigger:              trigger,
		},
	}
}

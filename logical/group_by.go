package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Trigger interface {
	Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment, keyTimeIndex int) physical.Trigger
}

type CountingTrigger struct {
	Count uint
}

func NewCountingTrigger(count uint) *CountingTrigger {
	return &CountingTrigger{Count: count}
}

func (w *CountingTrigger) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment, keyTimeIndex int) physical.Trigger {
	return physical.Trigger{
		TriggerType: physical.TriggerTypeCounting,
		CountingTrigger: &physical.CountingTrigger{
			TriggerAfter: w.Count,
		},
	}
}

type DelayTrigger struct {
	Delay Expression
}

func NewDelayTrigger(delay Expression) *DelayTrigger {
	return &DelayTrigger{Delay: delay}
}

func (w *DelayTrigger) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment, keyTimeIndex int) physical.Trigger {
	panic("implement me")
}

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (w *WatermarkTrigger) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment, keyTimeIndex int) physical.Trigger {
	if keyTimeIndex == -1 {
		panic(fmt.Errorf("can't use watermark trigger when not grouping by time field"))
	}
	return physical.Trigger{
		TriggerType: physical.TriggerTypeWatermark,
		WatermarkTrigger: &physical.WatermarkTrigger{
			TimeFieldIndex: keyTimeIndex,
		},
	}
}

type GroupBy struct {
	source   Node
	key      []Expression
	keyNames []string

	expressions    []Expression
	aggregates     []string
	aggregateNames []string

	triggers []Trigger
}

func NewGroupBy(source Node, key []Expression, keyNames []string, expressions []Expression, aggregates []string, aggregateNames []string, triggers []Trigger) *GroupBy {
	return &GroupBy{source: source, key: key, keyNames: keyNames, expressions: expressions, aggregates: aggregates, aggregateNames: aggregateNames, triggers: triggers}
}

func (node *GroupBy) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)

	keyEventTimeIndex := -1

	key := make([]physical.Expression, len(node.key))
	for i := range node.key {
		key[i] = node.key[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv.WithRecordUniqueVariableNames(mapping))
		if source.Schema.TimeField != -1 &&
			key[i].ExpressionType == physical.ExpressionTypeVariable &&
			physical.VariableNameMatchesField(key[i].Variable.Name, source.Schema.Fields[source.Schema.TimeField].Name) {

			keyEventTimeIndex = i
		}
	}

	expressions := make([]physical.Expression, len(node.expressions))
	for i := range node.expressions {
		expressions[i] = node.expressions[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv.WithRecordUniqueVariableNames(mapping))
	}

	aggregates := make([]physical.Aggregate, len(node.aggregates))
aggregateLoop:
	for i, aggname := range node.aggregates {
		descriptors := env.Aggregates[aggname]
		for _, descriptor := range descriptors {
			if descriptor.TypeFn != nil {
				if outputType, ok := descriptor.TypeFn(expressions[i].Type); ok {
					if octosql.Null.Is(expressions[i].Type) == octosql.TypeRelationIs {
						outputType = octosql.TypeSum(outputType, octosql.Null)
					}

					aggregates[i] = physical.Aggregate{
						Name:                node.aggregates[i],
						OutputType:          outputType,
						AggregateDescriptor: descriptor,
					}
					continue aggregateLoop
				}
			} else if expressions[i].Type.Is(octosql.TypeSum(descriptor.ArgumentType, octosql.Null)) == octosql.TypeRelationIs {
				outputType := descriptor.OutputType
				if octosql.Null.Is(expressions[i].Type) == octosql.TypeRelationIs {
					outputType = octosql.TypeSum(descriptor.OutputType, octosql.Null)
				}

				aggregates[i] = physical.Aggregate{
					Name:                node.aggregates[i],
					OutputType:          outputType,
					AggregateDescriptor: descriptor,
				}
				continue aggregateLoop
			}
		}
		for _, descriptor := range descriptors {
			if expressions[i].Type.Is(octosql.TypeSum(descriptor.ArgumentType, octosql.Null)) == octosql.TypeRelationMaybe {
				assertedExprType := *octosql.TypeIntersection(octosql.TypeSum(descriptor.ArgumentType, octosql.Null), expressions[i].Type)
				expressions[i] = physical.Expression{
					ExpressionType: physical.ExpressionTypeTypeAssertion,
					Type:           assertedExprType,
					TypeAssertion: &physical.TypeAssertion{
						Expression: expressions[i],
						TargetType: descriptor.ArgumentType,
					},
				}

				outputType := descriptor.OutputType
				if octosql.Null.Is(assertedExprType) == octosql.TypeRelationIs {
					outputType = octosql.TypeSum(descriptor.OutputType, octosql.Null)
				}

				aggregates[i] = physical.Aggregate{
					Name:                node.aggregates[i],
					OutputType:          outputType,
					AggregateDescriptor: descriptor,
				}
				continue aggregateLoop
			}
		}
		panic(fmt.Sprintf("unknown aggregate: %s(%s)", aggname, expressions[i].Type))
	}

	triggers := make([]physical.Trigger, len(node.triggers))
	for i := range node.triggers {
		triggers[i] = node.triggers[i].Typecheck(ctx, env, logicalEnv.WithRecordUniqueVariableNames(mapping), keyEventTimeIndex)
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
	outMapping := make(map[string]string)
	for i := range key {
		unique := logicalEnv.GetUnique(node.keyNames[i])
		outMapping[node.keyNames[i]] = unique
		schemaFields[i] = physical.SchemaField{
			Name: unique,
			Type: key[i].Type,
		}
	}
	for i := range aggregates {
		unique := logicalEnv.GetUnique(node.aggregateNames[i])
		outMapping[node.aggregateNames[i]] = unique
		schemaFields[len(key)+i] = physical.SchemaField{
			Name: unique,
			Type: aggregates[i].OutputType,
		}
	}

	return physical.Node{
		Schema:   physical.NewSchema(schemaFields, keyEventTimeIndex),
		NodeType: physical.NodeTypeGroupBy,
		GroupBy: &physical.GroupBy{
			Source:               source,
			Aggregates:           aggregates,
			AggregateExpressions: expressions,
			Key:                  key,
			KeyEventTimeIndex:    keyEventTimeIndex,
			Trigger:              trigger,
		},
	}, outMapping
}

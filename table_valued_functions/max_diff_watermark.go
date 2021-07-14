package table_valued_functions

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var MaxDiffWatermark = []physical.TableValuedFunctionDescriptor{
	{
		Arguments: map[string]physical.TableValuedFunctionArgumentMatcher{
			"source": {
				Required:                               true,
				TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeTable,
				Table:                                  &physical.TableValuedFunctionArgumentMatcherTable{},
			},
			"max_diff": {
				Required:                               true,
				TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
				Expression: &physical.TableValuedFunctionArgumentMatcherExpression{
					Type: octosql.Duration,
				},
			},
			"time_field": {
				Required:                               true,
				TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeDescriptor,
				Descriptor:                             &physical.TableValuedFunctionArgumentMatcherDescriptor{},
			},
		},
		OutputSchema: func(ctx context.Context, env physical.Environment, args map[string]physical.TableValuedFunctionArgument) (physical.Schema, error) {
			source := args["source"].Table.Table
			timeField := args["time_field"].Descriptor.Descriptor
			timeFieldIndex := -1
			for i, field := range source.Schema.Fields {
				if !physical.VariableNameMatchesField(timeField, field.Name) {
					continue
				}
				if field.Type.TypeID != octosql.TypeIDTime {
					return physical.Schema{}, fmt.Errorf("time_field must reference Time typed field, is %s", field.Type.String())
				}
				timeFieldIndex = i
				break
			}
			if timeFieldIndex == -1 {
				return physical.Schema{}, fmt.Errorf("no %s field in source stream", timeField)
			}
			return physical.Schema{
				Fields:    source.Schema.Fields,
				TimeField: timeFieldIndex,
			}, nil
		},
		Materialize: func(ctx context.Context, env physical.Environment, args map[string]physical.TableValuedFunctionArgument) (execution.Node, error) {
			source, err := args["source"].Table.Table.Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize source table: %w", err)
			}
			maxDifference, err := args["max_diff"].Expression.Expression.Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize max_diff: %w", err)
			}

			timeField := args["time_field"].Descriptor.Descriptor
			timeFieldIndex := -1
			for i, field := range args["source"].Table.Table.Schema.Fields {
				if physical.VariableNameMatchesField(timeField, field.Name) {
					timeFieldIndex = i
					break
				}
			}

			return &maxDifferenceWatermarkGenerator{
				source:         source,
				maxDifference:  maxDifference,
				timeFieldIndex: timeFieldIndex,
			}, nil
		},
	},
}

type maxDifferenceWatermarkGenerator struct {
	source         execution.Node
	maxDifference  execution.Expression
	timeFieldIndex int
}

func (m *maxDifferenceWatermarkGenerator) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	maxValue := time.Time{}
	curWatermark := time.Time{}

	maxDifference, err := m.maxDifference.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate max_diff: %w", err)
	}

	if err := m.source.Run(ctx, func(ctx execution.ProduceContext, record execution.Record) error {
		if record.Values[m.timeFieldIndex].Time.After(curWatermark) {
			record.EventTime = record.Values[m.timeFieldIndex].Time
			if err := produce(ctx, record); err != nil {
				return fmt.Errorf("couldn't produce record: %w", err)
			}
		}

		if curTimeValue := record.Values[m.timeFieldIndex].Time; curTimeValue.After(maxValue) {
			maxValue = curTimeValue
			curWatermark = curTimeValue.Add(-maxDifference.Duration)

			// TODO: Think about adding granularity here. (so i.e. only have second resolution)
			if err := metaSend(ctx, execution.MetadataMessage{
				Type:      execution.MetadataMessageTypeWatermark,
				Watermark: curWatermark,
			}); err != nil {
				return fmt.Errorf("couldn't send updated watermark")
			}
		}

		return nil
	}, func(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
		if msg.Type != execution.MetadataMessageTypeWatermark {
			return metaSend(ctx, msg)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	return nil
}

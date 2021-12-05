package table_valued_functions

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var MaxDiffWatermark = logical.TableValuedFunctionDescription{
	TypecheckArguments: func(ctx context.Context, env physical.Environment, logicalEnv logical.Environment, args map[string]logical.TableValuedFunctionArgumentValue) map[string]logical.TableValuedFunctionTypecheckedArgument {
		outArgs := make(map[string]logical.TableValuedFunctionTypecheckedArgument)

		source, mapping := args["source"].(*logical.TableValuedFunctionArgumentValueTable).
			Typecheck(ctx, env, logicalEnv)
		outArgs["source"] = logical.TableValuedFunctionTypecheckedArgument{Mapping: mapping, Argument: source}

		outArgs["max_diff"] = logical.TableValuedFunctionTypecheckedArgument{
			Argument: args["max_diff"].(*logical.TableValuedFunctionArgumentValueExpression).
				Typecheck(ctx, env, logicalEnv),
		}
		outArgs["time_field"] = logical.TableValuedFunctionTypecheckedArgument{
			Argument: args["time_field"].(*logical.TableValuedFunctionArgumentValueDescriptor).
				Typecheck(ctx, env, logicalEnv.WithRecordUniqueVariableNames(mapping)),
		}
		if _, ok := args["resolution"]; ok {
			outArgs["resolution"] = logical.TableValuedFunctionTypecheckedArgument{
				Argument: args["resolution"].(*logical.TableValuedFunctionArgumentValueExpression).
					Typecheck(ctx, env, logicalEnv),
			}
		}

		return outArgs
	},
	Descriptors: []logical.TableValuedFunctionDescriptor{
		{
			Arguments: map[string]logical.TableValuedFunctionArgumentMatcher{
				"source": {
					Required:                               true,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeTable,
					Table:                                  &logical.TableValuedFunctionArgumentMatcherTable{},
				},
				"max_diff": {
					Required:                               true,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
					Expression: &logical.TableValuedFunctionArgumentMatcherExpression{
						Type: octosql.Duration,
					},
				},
				"time_field": {
					Required:                               true,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeDescriptor,
					Descriptor:                             &logical.TableValuedFunctionArgumentMatcherDescriptor{},
				},
				"resolution": {
					Required:                               false,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
					Expression: &logical.TableValuedFunctionArgumentMatcherExpression{
						Type: octosql.Duration,
					},
				},
			},
			OutputSchema: func(ctx context.Context, env physical.Environment, logicalEnv logical.Environment, args map[string]logical.TableValuedFunctionTypecheckedArgument) (physical.Schema, map[string]string, error) {
				source := args["source"].Argument.Table.Table
				timeField := args["time_field"].Argument.Descriptor.Descriptor
				timeFieldIndex := -1
				for i, field := range source.Schema.Fields {
					if timeField != field.Name {
						continue
					}
					if field.Type.TypeID != octosql.TypeIDTime {
						return physical.Schema{}, nil, fmt.Errorf("time_field must reference field with type Time, is %s", field.Type.String())
					}
					timeFieldIndex = i
					break
				}
				if timeFieldIndex == -1 {
					return physical.Schema{}, nil, fmt.Errorf("no %s field in source stream", timeField)
				}
				return physical.Schema{
					Fields:    source.Schema.Fields,
					TimeField: timeFieldIndex,
				}, args["source"].Mapping, nil
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
					if timeField == field.Name {
						timeFieldIndex = i
						break
					}
				}
				var resolution execution.Expression = execution.NewConstant(octosql.NewDuration(time.Second))
				if arg, ok := args["resolution"]; ok {
					resolution, err = arg.Expression.Expression.Materialize(ctx, env)
					if err != nil {
						return nil, fmt.Errorf("couldn't materialize resolution: %w", err)
					}
				}

				return &maxDifferenceWatermarkGenerator{
					source:         source,
					maxDifference:  maxDifference,
					resolution:     resolution,
					timeFieldIndex: timeFieldIndex,
				}, nil
			},
		},
	},
}

type maxDifferenceWatermarkGenerator struct {
	source         execution.Node
	maxDifference  execution.Expression
	resolution     execution.Expression
	timeFieldIndex int
}

func (m *maxDifferenceWatermarkGenerator) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	maxValue := time.Time{}
	curWatermark := time.Time{}

	maxDifference, err := m.maxDifference.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate max_diff: %w", err)
	}
	resolution, err := m.resolution.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate resolution: %w", err)
	}

	if err := m.source.Run(ctx, func(ctx execution.ProduceContext, record execution.Record) error {
		if record.Values[m.timeFieldIndex].Time.After(curWatermark) {
			record.EventTime = record.Values[m.timeFieldIndex].Time
			if err := produce(ctx, record); err != nil {
				return fmt.Errorf("couldn't produce record: %w", err)
			}
		}

		curTimeValueRoundedDown := time.Unix(0, record.Values[m.timeFieldIndex].Time.UnixNano()/int64(resolution.Duration)*int64(resolution.Duration))

		if curTimeValueRoundedDown.After(maxValue) {
			maxValue = curTimeValueRoundedDown
			curWatermark = curTimeValueRoundedDown.Add(-maxDifference.Duration)

			if err := metaSend(ctx, execution.MetadataMessage{
				Type:      execution.MetadataMessageTypeWatermark,
				Watermark: curWatermark,
			}); err != nil {
				return fmt.Errorf("couldn't send updated watermark: %w", err)
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

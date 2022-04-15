package table_valued_functions

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var Tumble = logical.TableValuedFunctionDescription{
	TypecheckArguments: func(ctx context.Context, env physical.Environment, logicalEnv logical.Environment, args map[string]logical.TableValuedFunctionArgumentValue) map[string]logical.TableValuedFunctionTypecheckedArgument {
		outArgs := make(map[string]logical.TableValuedFunctionTypecheckedArgument)

		source, mapping := args["source"].(*logical.TableValuedFunctionArgumentValueTable).
			Typecheck(ctx, env, logicalEnv)
		outArgs["source"] = logical.TableValuedFunctionTypecheckedArgument{Mapping: mapping, Argument: source}

		outArgs["window_length"] = logical.TableValuedFunctionTypecheckedArgument{
			Argument: args["window_length"].(*logical.TableValuedFunctionArgumentValueExpression).
				Typecheck(ctx, env, logicalEnv),
		}
		if _, ok := args["time_field"]; ok {
			outArgs["time_field"] = logical.TableValuedFunctionTypecheckedArgument{
				Argument: args["time_field"].(*logical.TableValuedFunctionArgumentValueDescriptor).
					Typecheck(ctx, env, logicalEnv.WithRecordUniqueVariableNames(mapping)),
			}
		}
		if _, ok := args["offset"]; ok {
			outArgs["offset"] = logical.TableValuedFunctionTypecheckedArgument{
				Argument: args["offset"].(*logical.TableValuedFunctionArgumentValueExpression).
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
				"window_length": {
					Required:                               true,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
					Expression: &logical.TableValuedFunctionArgumentMatcherExpression{
						Type: octosql.Duration,
					},
				},
				"time_field": {
					Required:                               false,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeDescriptor,
					Descriptor:                             &logical.TableValuedFunctionArgumentMatcherDescriptor{},
				},
				"offset": {
					Required:                               false,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
					Expression: &logical.TableValuedFunctionArgumentMatcherExpression{
						Type: octosql.Duration,
					},
				},
			},
			OutputSchema: func(ctx context.Context, env physical.Environment, logicalEnv logical.Environment, args map[string]logical.TableValuedFunctionTypecheckedArgument) (physical.Schema, map[string]string, error) {
				source := args["source"].Argument.Table.Table
				if timeFieldDescriptor, ok := args["time_field"]; ok {
					timeField := timeFieldDescriptor.Argument.Descriptor.Descriptor
					found := false
					for _, field := range source.Schema.Fields {
						if field.Name != timeField {
							continue
						}
						if field.Type.TypeID != octosql.TypeIDTime {
							return physical.Schema{}, nil, fmt.Errorf("time_field must reference Time typed field, is %s", field.Type.String())
						}
						found = true
						break
					}
					if !found {
						return physical.Schema{}, nil, fmt.Errorf("no %s field in source stream", timeField)
					}
				} else {
					if source.Schema.TimeField == -1 {
						return physical.Schema{}, nil, fmt.Errorf("the source table has no implicit watermarked time field, time_field must be specified explicitly")
					}
				}
				outMapping := make(map[string]string)
				for k, v := range args["source"].Mapping {
					outMapping[k] = v
				}
				outFields := make([]physical.SchemaField, len(source.Schema.Fields)+2)
				copy(outFields, source.Schema.Fields)

				uniqueWindowStart := logicalEnv.GetUnique("window_start")
				outMapping["window_start"] = uniqueWindowStart
				outFields[len(source.Schema.Fields)] = physical.SchemaField{
					Name: uniqueWindowStart,
					Type: octosql.Time,
				}

				uniqueWindowEnd := logicalEnv.GetUnique("window_end")
				outMapping["window_end"] = uniqueWindowEnd
				outFields[len(source.Schema.Fields)+1] = physical.SchemaField{
					Name: uniqueWindowEnd,
					Type: octosql.Time,
				}
				return physical.Schema{
					Fields:        outFields,
					TimeField:     len(source.Schema.Fields) + 1,
					NoRetractions: source.Schema.NoRetractions,
				}, outMapping, nil
			},
			Materialize: func(ctx context.Context, env physical.Environment, args map[string]physical.TableValuedFunctionArgument) (execution.Node, error) {
				source, err := args["source"].Table.Table.Materialize(ctx, env)
				if err != nil {
					return nil, fmt.Errorf("couldn't materialize source table: %w", err)
				}
				windowLength, err := args["window_length"].Expression.Expression.Materialize(ctx, env)
				if err != nil {
					return nil, fmt.Errorf("couldn't materialize window_length: %w", err)
				}
				var timeFieldIndex int
				if timeFieldDescriptor, ok := args["time_field"]; ok {
					timeField := timeFieldDescriptor.Descriptor.Descriptor
					for i, field := range args["source"].Table.Table.Schema.Fields {
						if field.Name == timeField {
							timeFieldIndex = i
							break
						}
					}
				} else {
					timeFieldIndex = args["source"].Table.Table.Schema.TimeField
				}
				var offset execution.Expression
				if offsetExpr, ok := args["offset"]; ok {
					offset, err = offsetExpr.Expression.Expression.Materialize(ctx, env)
					if err != nil {
						return nil, fmt.Errorf("couldn't materialize offset: %w", err)
					}
				} else {
					offset = execution.NewConstant(octosql.NewDuration(0))
				}

				return &tumble{
					source:         source,
					timeFieldIndex: timeFieldIndex,
					windowLength:   windowLength,
					offset:         offset,
				}, nil
			},
		},
	},
}

type tumble struct {
	source         execution.Node
	timeFieldIndex int
	windowLength   execution.Expression
	offset         execution.Expression
}

func (t *tumble) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	windowLength, err := t.windowLength.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate window_length: %w", err)
	}
	offset, err := t.offset.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate offset: %w", err)
	}

	if err := t.source.Run(ctx, func(ctx execution.ProduceContext, record execution.Record) error {
		timeValue := record.Values[t.timeFieldIndex].Time
		windowStart := timeValue.Add(-1 * offset.Duration).Truncate(windowLength.Duration).Add(offset.Duration)
		windowEnd := windowStart.Add(windowLength.Duration)
		record.Values = append(record.Values, octosql.NewTime(windowStart), octosql.NewTime(windowEnd))

		if err := produce(ctx, record); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}

		return nil
	}, metaSend); err != nil {
		return fmt.Errorf("couldn't run source: %w", err)
	}

	return nil
}

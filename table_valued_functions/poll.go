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

var Poll = logical.TableValuedFunctionDescription{
	TypecheckArguments: func(ctx context.Context, env physical.Environment, logicalEnv logical.Environment, args map[string]logical.TableValuedFunctionArgumentValue) map[string]logical.TableValuedFunctionTypecheckedArgument {
		outArgs := make(map[string]logical.TableValuedFunctionTypecheckedArgument)

		source, mapping := args["source"].(*logical.TableValuedFunctionArgumentValueTable).
			Typecheck(ctx, env, logicalEnv)
		outArgs["source"] = logical.TableValuedFunctionTypecheckedArgument{Mapping: mapping, Argument: source}

		if _, ok := args["poll_interval"]; ok {
			outArgs["poll_interval"] = logical.TableValuedFunctionTypecheckedArgument{
				Argument: args["poll_interval"].(*logical.TableValuedFunctionArgumentValueDescriptor).
					Typecheck(ctx, env, logicalEnv.WithRecordUniqueVariableNames(mapping)),
				// TODO: Check that this is actually a duration.
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
				"poll_interval": {
					Required:                               false,
					TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeDescriptor,
					Descriptor:                             &logical.TableValuedFunctionArgumentMatcherDescriptor{},
				},
			},
			OutputSchema: func(ctx context.Context, env physical.Environment, logicalEnv logical.Environment, args map[string]logical.TableValuedFunctionTypecheckedArgument) (physical.Schema, map[string]string, error) {
				source := args["source"].Argument.Table.Table

				outFields := make([]physical.SchemaField, len(source.Schema.Fields)+1)
				copy(outFields[1:], source.Schema.Fields)

				outMapping := make(map[string]string)
				for k, v := range args["source"].Mapping {
					outMapping[k] = v
				}

				uniqueTime := logicalEnv.GetUnique("time")
				outMapping["time"] = uniqueTime
				outFields[0] = physical.SchemaField{
					Name: uniqueTime,
					Type: octosql.Time,
				}

				return physical.Schema{
					Fields:    outFields,
					TimeField: 0,
				}, outMapping, nil
			},
			Materialize: func(ctx context.Context, env physical.Environment, args map[string]physical.TableValuedFunctionArgument) (execution.Node, error) {
				source, err := args["source"].Table.Table.Materialize(ctx, env)
				if err != nil {
					return nil, fmt.Errorf("couldn't materialize source table: %w", err)
				}
				var interval execution.Expression
				if intervalExpr, ok := args["poll_interval"]; ok {
					interval, err = intervalExpr.Expression.Expression.Materialize(ctx, env)
					if err != nil {
						return nil, fmt.Errorf("couldn't materialize interval: %w", err)
					}
				} else {
					interval = execution.NewConstant(octosql.NewDuration(time.Second))
				}

				return &poll{
					source:   source,
					interval: interval,
				}, nil
			},
		},
	},
}

type poll struct {
	source   execution.Node
	interval execution.Expression
}

func (t *poll) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	panic("implement me")
	// offset, err := t.interval.Evaluate(ctx)
	// if err != nil {
	// 	return fmt.Errorf("couldn't evaluate offset: %w", err)
	// }
	//
	// var lastNow time.Time
	// var lastValues [][]octosql.Value
	//
	// for {
	// 	now := time.Now()
	//
	// 	if !lastNow.IsZero() {
	// 		for i := range lastValues {
	// 			if err := produce(
	// 				execution.ProduceFromExecutionContext(ctx),
	// 				execution.NewRecordBatch(lastValues[i], true, lastNow),
	// 			); err != nil {
	// 				return fmt.Errorf("couldn't produce record: %w", err)
	// 			}
	// 		}
	// 	}
	//
	// 	lastNow = now
	// 	lastValues = nil
	//
	// 	if err := t.source.Run(ctx, func(ctx execution.ProduceContext, record execution.RecordBatch) error {
	// 		values := make([]octosql.Value, len(record.Values)+1)
	// 		copy(values[1:], record.Values)
	// 		values[0] = octosql.NewTime(now)
	//
	// 		lastValues = append(lastValues, make([]octosql.Value, len(values)))
	// 		copy(lastValues[len(lastValues)-1], values)
	//
	// 		if err := produce(ctx, execution.NewRecordBatch(values, false, now)); err != nil {
	// 			return fmt.Errorf("couldn't produce record: %w", err)
	// 		}
	//
	// 		return nil
	// 	}, metaSend); err != nil {
	// 		return fmt.Errorf("couldn't run source: %w", err)
	// 	}
	//
	// 	if err := metaSend(execution.ProduceFromExecutionContext(ctx), execution.MetadataMessage{
	// 		Type:      execution.MetadataMessageTypeWatermark,
	// 		Watermark: now,
	// 	}); err != nil {
	// 		return fmt.Errorf("couldn't send updated watermark: %w", err)
	// 	}
	//
	// 	time.Sleep(offset.Duration)
	// }
	//
	// return nil
}

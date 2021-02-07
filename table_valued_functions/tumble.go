package table_valued_functions

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var Tumble = physical.TableValuedFunctionDescriptor{
	Arguments: map[string]physical.TableValuedFunctionArgumentMatcher{
		"source": {
			Required:                               true,
			TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeTable,
			Table:                                  &physical.TableValuedFunctionArgumentMatcherTable{},
		},
		"window_length": {
			Required:                               true,
			TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
			Expression: &physical.TableValuedFunctionArgumentMatcherExpression{
				Type: octosql.Duration,
			},
		},
		"time_field": {
			Required:                               false,
			TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeDescriptor,
			Descriptor:                             &physical.TableValuedFunctionArgumentMatcherDescriptor{},
		},
		"offset": {
			Required:                               false,
			TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
			Expression: &physical.TableValuedFunctionArgumentMatcherExpression{
				Type: octosql.Duration,
			},
		},
	},
	OutputSchema: func(ctx context.Context, env physical.Environment, args map[string]physical.TableValuedFunctionArgument) (physical.Schema, error) {
		source := args["source"].Table.Table
		if timeField, ok := args["time_field"]; ok {
			for _, field := range source.Schema.Fields {
				if !physical.VariableNameMatchesField(timeField.Descriptor.Descriptor, field.Name) {
					continue
				}
				if field.Type.TypeID != octosql.TypeIDTime {
					return physical.Schema{}, fmt.Errorf("time_field must reference Time typed field, is %s", field.Type.String())
				}
				break
			}
		} else {
			if source.Schema.TimeField == -1 {
				return physical.Schema{}, fmt.Errorf("the source table has no implicit watermarked time field, time_field must be specified explicitly")
			}
		}
		outFields := make([]physical.SchemaField, len(source.Schema.Fields)+2)
		copy(outFields, source.Schema.Fields)
		outFields[len(source.Schema.Fields)] = physical.SchemaField{
			Name: "window_start",
			Type: octosql.Time,
		}
		outFields[len(source.Schema.Fields)+1] = physical.SchemaField{
			Name: "window_end",
			Type: octosql.Time,
		}
		return physical.Schema{
			Fields:    outFields,
			TimeField: len(source.Schema.Fields) + 1,
		}, nil
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
		if timeField, ok := args["time_field"]; ok {
			for i, field := range args["source"].Table.Table.Schema.Fields {
				if physical.VariableNameMatchesField(timeField.Descriptor.Descriptor, field.Name) {
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

		return nodes.NewTumble(source, timeFieldIndex, windowLength, offset), nil
	},
}

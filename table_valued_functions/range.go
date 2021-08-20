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

var Range = []logical.TableValuedFunctionDescriptor{
	{
		Arguments: map[string]logical.TableValuedFunctionArgumentMatcher{
			"start": {
				Required:                               true,
				TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
				Expression: &logical.TableValuedFunctionArgumentMatcherExpression{
					Type: octosql.Int,
				},
			},
			"up_to": {
				Required:                               true,
				TableValuedFunctionArgumentMatcherType: physical.TableValuedFunctionArgumentTypeExpression,
				Expression: &logical.TableValuedFunctionArgumentMatcherExpression{
					Type: octosql.Int,
				},
			},
		},
		OutputSchema: func(
			ctx context.Context,
			env physical.Environment,
			logicalEnv logical.Environment,
			args map[string]physical.TableValuedFunctionArgument,
		) (physical.Schema, map[string]string, error) {
			unique := logicalEnv.GetUnique("i")

			return physical.Schema{
					Fields: []physical.SchemaField{
						{
							Name: unique,
							Type: octosql.Int,
						},
					},
					TimeField: -1,
				}, map[string]string{
					"i": unique,
				}, nil
		},
		Materialize: func(
			ctx context.Context,
			environment physical.Environment,
			args map[string]physical.TableValuedFunctionArgument,
		) (execution.Node, error) {
			start, err := args["start"].Expression.Expression.Materialize(ctx, environment)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize start: %w", err)
			}
			end, err := args["up_to"].Expression.Expression.Materialize(ctx, environment)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize end: %w", err)
			}

			return &rangeNode{
				start: start,
				end:   end,
			}, err
		},
	},
}

type rangeNode struct {
	start, end execution.Expression
}

func (r *rangeNode) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	start, err := r.start.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate start: %w", err)
	}
	end, err := r.end.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate end: %w", err)
	}
	for i := start.Int; i < end.Int; i++ {
		if err := produce(
			execution.ProduceFromExecutionContext(ctx),
			execution.NewRecord([]octosql.Value{octosql.NewInt(i)}, false, time.Time{}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}

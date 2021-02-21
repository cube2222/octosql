package slicequery

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/slicedb"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/table_valued_functions"
)

type Record execution.Record

func (record Record) String() string {
	return execution.Record(record).String()
}

func Query(ctx context.Context, tables map[string]interface{}, query string, onEachRecord func(ctx context.Context, record Record) error) error {
	databases := make(map[string]physical.Database)
	// TODO: Also have a configurable datasource name resolver. Here we can set it to use the args table by default.
	db, err := slicedb.NewDatabase(tables)
	if err != nil {
		return fmt.Errorf("couldn't initialize slice database")
	}
	databases["args"] = db

	statement, err := sqlparser.Parse(query)
	if err != nil {
		return fmt.Errorf("couldn't parse query: %w", err)
	}
	logicalPlan, err := parser.ParseNode(statement.(sqlparser.SelectStatement))
	if err != nil {
		return fmt.Errorf("couldn't parse query: %w", err)
	}
	env := physical.Environment{
		Aggregates: map[string][]physical.AggregateDescriptor{
			"count":          aggregates.CountOverloads,
			"count_distinct": aggregates.DistinctAggregateOverloads(aggregates.CountOverloads),
			"sum":            aggregates.SumOverloads,
			"sum_distinct":   aggregates.DistinctAggregateOverloads(aggregates.SumOverloads),
			"avg":            aggregates.AverageOverloads,
			"avg_distinct":   aggregates.DistinctAggregateOverloads(aggregates.AverageOverloads),
			"max":            aggregates.MaxOverloads,
			"min":            aggregates.MinOverloads,
		},
		Functions: functions.FunctionMap(),
		Datasources: &physical.DatasourceRepository{
			Databases: databases,
			FileHandlers: map[string]func(name string) (physical.DatasourceImplementation, error){
				"json": json.Creator,
				"csv":  csv.Creator,
			},
		},
		TableValuedFunctions: map[string][]physical.TableValuedFunctionDescriptor{
			"max_diff_watermark": table_valued_functions.MaxDiffWatermark,
			"tumble":             table_valued_functions.Tumble,
		},
		PhysicalConfig:  nil,
		VariableContext: nil,
	}
	// TODO: Wrap panics into errors in subfunction.
	var physicalPlan physical.Node
	func() {
		defer func() {
			if msg := recover(); msg != nil {
				err = fmt.Errorf("typecheck error: %s", msg)
			}
		}()
		physicalPlan = logicalPlan.Typecheck(
			context.Background(),
			env,
			logical.Environment{
				CommonTableExpressions: map[string]physical.Node{},
			},
		)
	}()
	if err != nil {
		return err
	}
	spew.Dump(physicalPlan.Schema)
	physicalPlan = optimizer.Optimize(physicalPlan)
	executionPlan, err := physicalPlan.Materialize(
		context.Background(),
		env,
	)
	if err != nil {
		return fmt.Errorf("couldn't materialize physical plan: %w", err)
	}
	if err := executionPlan.Run(
		execution.ExecutionContext{
			Context:         context.Background(),
			VariableContext: nil,
		},
		func(ctx execution.ProduceContext, record execution.Record) error {
			return onEachRecord(ctx.Context, Record(record))
		},
		func(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
			return nil
		},
	); err != nil {
		return fmt.Errorf("couldn't run query: %w", err)
	}

	return nil
}

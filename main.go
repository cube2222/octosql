package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/postgres"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/outputs/batch"
	"github.com/cube2222/octosql/outputs/stream"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/table_valued_functions"
)

func main() {
	databaseCreators := map[string]func(ctx context.Context, configUntyped config.DatabaseSpecificConfig) (physical.Database, error){
		"postgres": postgres.Creator,
	}

	config.RegisterDatabaseType("postgres", func() config.DatabaseSpecificConfig { return &postgres.Config{} })
	cfg, err := config.Read()
	if err != nil {
		log.Fatal(err)
	}

	databases := make(map[string]physical.Database)
	for _, dbConfig := range cfg.Databases {
		db, err := databaseCreators[dbConfig.Type](context.Background(), dbConfig.Config)
		if err != nil {
			log.Fatal(err)
		}
		databases[dbConfig.Name] = db
	}

	statement, err := sqlparser.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	logicalPlan, outputOptions, err := parser.ParseNode(statement.(sqlparser.SelectStatement), true)
	if err != nil {
		log.Fatal(err)
	}
	env := physical.Environment{
		Aggregates: map[string][]physical.AggregateDescriptor{
			"array_agg":          aggregates.ArrayOverloads,
			"array_agg_distinct": aggregates.DistinctAggregateOverloads(aggregates.ArrayOverloads),
			"count":              aggregates.CountOverloads,
			"count_distinct":     aggregates.DistinctAggregateOverloads(aggregates.CountOverloads),
			"sum":                aggregates.SumOverloads,
			"sum_distinct":       aggregates.DistinctAggregateOverloads(aggregates.SumOverloads),
			"avg":                aggregates.AverageOverloads,
			"avg_distinct":       aggregates.DistinctAggregateOverloads(aggregates.AverageOverloads),
			"max":                aggregates.MaxOverloads,
			"min":                aggregates.MinOverloads,
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
			"range":              table_valued_functions.Range,
		},
		PhysicalConfig:  nil,
		VariableContext: nil,
	}
	// TODO: Wrap panics into errors in subfunction.
	physicalPlan := logicalPlan.Typecheck(
		context.Background(),
		env,
		logical.Environment{
			CommonTableExpressions: map[string]physical.Node{},
		},
	)
	spew.Dump(physicalPlan.Schema)
	start := time.Now()
	physicalPlan = optimizer.Optimize(physicalPlan)
	log.Printf("time for optimisation: %s", time.Since(start))
	executionPlan, err := physicalPlan.Materialize(
		context.Background(),
		env,
	)
	if err != nil {
		log.Fatal(err)
	}
	// TODO: Each setup costs us a DESCRIBE, so caching is really a must. Specifically, caching of a materialized plan.
	log.Printf("time to materialization: %s", time.Since(start))

	orderByExpressions := make([]execution.Expression, len(outputOptions.OrderByExpressions))
	for i := range outputOptions.OrderByExpressions {
		physicalExpr := outputOptions.OrderByExpressions[i].Typecheck(context.Background(), env.WithRecordSchema(physicalPlan.Schema), logical.Environment{})
		execExpr, err := physicalExpr.Materialize(context.Background(), env.WithRecordSchema(physicalPlan.Schema))
		if err != nil {
			log.Fatalf("couldn't materialize output order by expression with index %d: %v", i, err)
		}
		orderByExpressions[i] = execExpr
	}
	orderByDirections := logical.DirectionsToMultipliers(outputOptions.OrderByDirections)

	var sink interface {
		Run(execCtx execution.ExecutionContext) error
	}

	switch os.Getenv("OCTOSQL_OUTPUT") {
	case "live_table":
		sink = batch.NewOutputPrinter(
			executionPlan,
			orderByExpressions,
			logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			outputOptions.Limit,
			physicalPlan.Schema,
			batch.NewTableFormatter,
			true,
		)
	case "batch_table":
		sink = batch.NewOutputPrinter(
			executionPlan,
			orderByExpressions,
			logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			outputOptions.Limit,
			physicalPlan.Schema,
			batch.NewTableFormatter,
			false,
		)
	case "stream_native":
		if len(orderByExpressions) > 0 {
			executionPlan = nodes.NewBatchOrderBy(
				executionPlan,
				orderByExpressions,
				orderByDirections,
			)
		}
		if outputOptions.Limit > 0 {
			log.Fatal("LIMIT clause not supported with stream output.")
		}

		sink = stream.NewOutputPrinter(
			executionPlan,
			stream.NewNativeFormat(physicalPlan.Schema),
		)
	default:
		sink = batch.NewOutputPrinter(
			executionPlan,
			orderByExpressions,
			logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			outputOptions.Limit,
			physicalPlan.Schema,
			batch.NewTableFormatter,
			false,
		)
	}

	if err := sink.Run(
		execution.ExecutionContext{
			Context:         context.Background(),
			VariableContext: nil,
		},
	); err != nil {
		log.Fatal(err)
	}
}

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/skratchdot/open-golang/open"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/postgres"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/helpers/graph"
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
	describe := flag.Int("describe", 0, "")
	if err := flag.CommandLine.Parse(os.Args[2:]); err != nil {
		log.Fatal(err)
	}

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
		PhysicalConfig:  nil,
		VariableContext: nil,
	}
	// TODO: Wrap panics into errors in subfunction.
	tableValuedFunctions := map[string][]logical.TableValuedFunctionDescriptor{
		"max_diff_watermark": table_valued_functions.MaxDiffWatermark,
		"tumble":             table_valued_functions.Tumble,
		"range":              table_valued_functions.Range,
	}
	uniqueNameGenerator := map[string]int{}
	physicalPlan, mapping := logicalPlan.Typecheck(
		context.Background(),
		env,
		logical.Environment{
			CommonTableExpressions: map[string]logical.CommonTableExpression{},
			TableValuedFunctions:   tableValuedFunctions,
			UniqueNameGenerator:    uniqueNameGenerator,
		},
	)
	spew.Dump(physicalPlan.Schema)
	start := time.Now()
	physicalPlan = optimizer.Optimize(physicalPlan)
	log.Printf("time for optimisation: %s", time.Since(start))

	if *describe >= 1 {
		file, err := os.CreateTemp(os.TempDir(), "octosql-describe-*.png")
		if err != nil {
			log.Fatal(err)
		}
		os.WriteFile("describe.txt", []byte(graph.Show(physical.DescribeNode(physicalPlan, true)).String()), os.ModePerm)
		cmd := exec.Command("dot", "-Tpng")
		cmd.Stdin = strings.NewReader(graph.Show(physical.DescribeNode(physicalPlan, *describe >= 2)).String())
		cmd.Stdout = file
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatal("couldn't render graph: ", err)
		}
		if err := file.Close(); err != nil {
			log.Fatal("couldn't close temporary file: ", err)
		}
		if err := open.Start(file.Name()); err != nil {
			log.Fatal("couldn't open graph: ", err)
		}
		return
	}

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
		physicalExpr := outputOptions.OrderByExpressions[i].Typecheck(context.Background(), env.WithRecordSchema(physicalPlan.Schema), logical.Environment{
			CommonTableExpressions: map[string]logical.CommonTableExpression{},
			TableValuedFunctions:   tableValuedFunctions,
			UniqueVariableNames: &logical.VariableMapping{
				Mapping: mapping,
			},
			UniqueNameGenerator: uniqueNameGenerator,
		})
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

	outFields := make([]physical.SchemaField, len(physicalPlan.Schema.Fields))
	copy(outFields, physicalPlan.Schema.Fields)
	outSchema := physical.Schema{
		Fields:    outFields,
		TimeField: physicalPlan.Schema.TimeField,
	}
	reverseMapping := logical.ReverseMapping(mapping)
	for i := range outFields {
		outFields[i].Name = reverseMapping[outFields[i].Name]
	}

	switch os.Getenv("OCTOSQL_OUTPUT") {
	case "live_table":
		sink = batch.NewOutputPrinter(
			executionPlan,
			orderByExpressions,
			logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			outputOptions.Limit,
			outSchema,
			batch.NewTableFormatter,
			true,
		)
	case "batch_table":
		sink = batch.NewOutputPrinter(
			executionPlan,
			orderByExpressions,
			logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			outputOptions.Limit,
			outSchema,
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
			stream.NewNativeFormat(outSchema),
		)
	default:
		sink = batch.NewOutputPrinter(
			executionPlan,
			orderByExpressions,
			logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			outputOptions.Limit,
			outSchema,
			batch.NewTableFormatter,
			true,
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

package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/Masterminds/semver"
	"github.com/skratchdot/open-golang/open"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/plugins"
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
	"github.com/cube2222/octosql/plugins/executor"
	"github.com/cube2222/octosql/plugins/manager"
	"github.com/cube2222/octosql/plugins/repository"
	"github.com/cube2222/octosql/table_valued_functions"
	"github.com/cube2222/octosql/telemetry"
)

var emptyYamlNode = func() yaml.Node {
	var out yaml.Node
	if err := yaml.Unmarshal([]byte("{}"), &out); err != nil {
		log.Fatalf("[BUG] Couldn't create empty yaml node: %s", err)
	}
	return out
}()

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "octosql",
	Args:  cobra.ExactArgs(1),
	Short: "",
	Long:  ``,
	Example: `octosql "SELECT * FROM myfile.json"
octosql "SELECT * FROM ` + "`mydir/myfile.json`" + `
octosql "SELECT * FROM plugins.plugins"`,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) (outErr error) {
		ctx := cmd.Context()
		debug.SetGCPercent(1000)

		pluginManager := &manager.PluginManager{}

		pluginExecutor := executor.PluginExecutor{
			Manager: pluginManager,
		}
		defer func() {
			if err := pluginExecutor.Close(); err != nil {
				if outErr == nil {
					outErr = fmt.Errorf("couldn't close plugin executor: %w", err)
				}
			}
		}()

		cfg, err := config.Read()
		if err != nil {
			return fmt.Errorf("couldn't read config: %w", err)
		}

		installedPlugins, err := pluginManager.ListInstalledPlugins()
		if err != nil {
			return fmt.Errorf("couldn't list installed plugins: %w", err)
		}

		resolvedVersions := map[string]*semver.Version{}

		// Fill in plugin versions.
	dbLoop:
		for i := range cfg.Databases {
			if cfg.Databases[i].Version == nil {
				constraint, _ := semver.NewConstraint("*")
				cfg.Databases[i].Version = config.NewYamlUnmarshallableVersionConstraint(constraint)
			}
			for _, plugin := range installedPlugins {
				if plugin.Reference != cfg.Databases[i].Type {
					continue
				}
				for _, version := range plugin.Versions {
					if cfg.Databases[i].Version.Raw().Check(version.Number) {
						resolvedVersions[cfg.Databases[i].Name] = version.Number
						continue dbLoop
					}
				}
				break
			}
			return fmt.Errorf("database '%s' plugin '%s' used in configuration is not instaled with the required version", cfg.Databases[i].Name, cfg.Databases[i].Type.String())
		}

		databases := make(map[string]func() (physical.Database, error))
		for _, dbConfig := range cfg.Databases {
			once := sync.Once{}
			curDbConfig := dbConfig
			var db physical.Database
			var err error

			databases[curDbConfig.Name] = func() (physical.Database, error) {
				once.Do(func() {
					db, err = pluginExecutor.RunPlugin(ctx, curDbConfig.Type, curDbConfig.Name, resolvedVersions[curDbConfig.Name], curDbConfig.Config)
				})
				if err != nil {
					return nil, fmt.Errorf("couldn't run %s plugin %s: %w", curDbConfig.Type.String(), curDbConfig.Name, err)
				}
				return db, nil
			}
		}
		{
			once := sync.Once{}
			var repositories []repository.Repository
			var err error
			databases["plugins"] = func() (physical.Database, error) {
				once.Do(func() {
					repositories, err = repository.GetRepositories(ctx)
				})
				if err != nil {
					return nil, fmt.Errorf("couldn't get repositories: %w", err)
				}
				return plugins.Creator(ctx, pluginManager, repositories)
			}
		}

		for _, metadata := range installedPlugins {
			if _, ok := databases[metadata.Reference.Name]; ok {
				continue
			}
			curMetadata := metadata

			once := sync.Once{}
			var db physical.Database
			var err error

			databases[curMetadata.Reference.Name] = func() (physical.Database, error) {
				once.Do(func() {
					db, err = pluginExecutor.RunPlugin(ctx, curMetadata.Reference, curMetadata.Reference.Name, metadata.Versions[0].Number, emptyYamlNode)
				})
				if err != nil {
					return nil, fmt.Errorf("couldn't run default plugin %s database: %w", curMetadata.Reference, err)
				}
				return db, nil
			}
		}
		env := physical.Environment{
			Aggregates: aggregates.Aggregates,
			Functions:  functions.FunctionMap(),
			Datasources: &physical.DatasourceRepository{
				Databases: databases,
				FileHandlers: map[string]func(name string) (physical.DatasourceImplementation, physical.Schema, error){
					"json": json.Creator,
					"csv":  csv.Creator,
				},
			},
			PhysicalConfig:  nil,
			VariableContext: nil,
		}
		statement, err := sqlparser.Parse(args[0])
		if err != nil {
			return fmt.Errorf("couldn't parse query: %w", err)
		}
		logicalPlan, outputOptions, err := parser.ParseNode(statement.(sqlparser.SelectStatement), true)
		if err != nil {
			return fmt.Errorf("couldn't parse query: %w", err)
		}
		tableValuedFunctions := map[string]logical.TableValuedFunctionDescription{
			"max_diff_watermark": table_valued_functions.MaxDiffWatermark,
			"tumble":             table_valued_functions.Tumble,
			"range":              table_valued_functions.Range,
			"poll":               table_valued_functions.Poll,
		}
		uniqueNameGenerator := map[string]int{}
		physicalPlan, mapping, err := typecheckNode(
			ctx,
			logicalPlan,
			env,
			logical.Environment{
				CommonTableExpressions: map[string]logical.CommonTableExpression{},
				TableValuedFunctions:   tableValuedFunctions,
				UniqueNameGenerator:    uniqueNameGenerator,
			},
		)
		if err != nil {
			return err
		}
		reverseMapping := logical.ReverseMapping(mapping)

		queryTelemetry := telemetry.GetQueryTelemetryData(physicalPlan, installedPlugins)

		var executionPlan execution.Node
		var orderByExpressions []execution.Expression
		var outSchema physical.Schema
		if describe {
			telemetry.SendTelemetry(ctx, "describe", queryTelemetry)

			for i := range physicalPlan.Schema.Fields {
				physicalPlan.Schema.Fields[i].Name = reverseMapping[physicalPlan.Schema.Fields[i].Name]
			}
			executionPlan = &DescribeNode{
				Schema: physicalPlan.Schema,
			}
			outSchema = DescribeNodeSchema
			outputOptions.Limit = 0
			outputOptions.OrderByExpressions = nil
			outputOptions.OrderByDirections = nil
		} else {
			telemetry.SendTelemetry(ctx, "query", queryTelemetry)

			if optimize {
				physicalPlan = optimizer.Optimize(physicalPlan)
			}

			if explain >= 1 {
				file, err := os.CreateTemp(os.TempDir(), "octosql-describe-*.png")
				if err != nil {
					return fmt.Errorf("couldn't create temporary file: %w", err)
				}
				os.WriteFile("describe.txt", []byte(graph.Show(physical.DescribeNode(physicalPlan, true)).String()), os.ModePerm)
				cmd := exec.Command("dot", "-Tpng")
				cmd.Stdin = strings.NewReader(graph.Show(physical.DescribeNode(physicalPlan, explain >= 2)).String())
				cmd.Stdout = file
				cmd.Stderr = os.Stderr
				if err := cmd.Run(); err != nil {
					return fmt.Errorf("couldn't render graph: %w", err)
				}
				if err := file.Close(); err != nil {
					return fmt.Errorf("couldn't close temporary file: %w", err)
				}
				if err := open.Start(file.Name()); err != nil {
					return fmt.Errorf("couldn't open graph: %w", err)
				}
				return
			}

			executionPlan, err = physicalPlan.Materialize(
				ctx,
				env,
			)
			if err != nil {
				return fmt.Errorf("couldn't materialize physical plan: %w", err)
			}

			orderByExpressions := make([]execution.Expression, len(outputOptions.OrderByExpressions))
			for i := range outputOptions.OrderByExpressions {
				physicalExpr, err := typecheckExpr(ctx, outputOptions.OrderByExpressions[i], env.WithRecordSchema(physicalPlan.Schema), logical.Environment{
					CommonTableExpressions: map[string]logical.CommonTableExpression{},
					TableValuedFunctions:   tableValuedFunctions,
					UniqueVariableNames: &logical.VariableMapping{
						Mapping: mapping,
					},
					UniqueNameGenerator: uniqueNameGenerator,
				})
				if err != nil {
					return fmt.Errorf("couldn't typecheck order by expression with index %d: %w", i, err)
				}
				execExpr, err := physicalExpr.Materialize(ctx, env.WithRecordSchema(physicalPlan.Schema))
				if err != nil {
					return fmt.Errorf("couldn't materialize output order by expression with index %d: %v", i, err)
				}
				orderByExpressions[i] = execExpr
			}

			outFields := make([]physical.SchemaField, len(physicalPlan.Schema.Fields))
			copy(outFields, physicalPlan.Schema.Fields)
			outSchema = physical.Schema{
				Fields:    outFields,
				TimeField: physicalPlan.Schema.TimeField,
			}
			for i := range outFields {
				outFields[i].Name = reverseMapping[outFields[i].Name]
			}
		}

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
					logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
				)
			}
			if outputOptions.Limit > 0 {
				return fmt.Errorf("LIMIT clause not supported with stream output")
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
				Context:         ctx,
				VariableContext: nil,
			},
		); err != nil {
			return fmt.Errorf("couldn't run query: %w", err)
		}
		return nil
	},
}

func Execute(ctx context.Context) {
	cobra.CheckErr(rootCmd.ExecuteContext(ctx))
}

var describe bool
var explain int
var optimize bool

func init() {
	rootCmd.Flags().BoolVar(&describe, "describe", false, "Describe query output schema.")
	rootCmd.Flags().IntVar(&explain, "explain", 0, "Describe query output schema.")
	rootCmd.Flags().BoolVar(&optimize, "optimize", true, "Whether OctoSQL should optimize the query.")
}

func typecheckNode(ctx context.Context, node logical.Node, env physical.Environment, logicalEnv logical.Environment) (_ physical.Node, _ map[string]string, outErr error) {
	defer func() {
		if r := recover(); r != nil {
			outErr = fmt.Errorf("typecheck error: %s", r)
		}
	}()
	physicalNode, mapping := node.Typecheck(
		ctx,
		env,
		logicalEnv,
	)
	return physicalNode, mapping, nil
}

func typecheckExpr(ctx context.Context, expr logical.Expression, env physical.Environment, logicalEnv logical.Environment) (_ physical.Expression, outErr error) {
	defer func() {
		if r := recover(); r != nil {
			outErr = fmt.Errorf("typecheck error: %s", r)
		}
	}()
	physicalExpr := expr.Typecheck(
		ctx,
		env,
		logicalEnv,
	)
	return physicalExpr, nil
}

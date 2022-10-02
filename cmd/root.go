package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime/debug"
	"runtime/trace"
	"strings"

	"github.com/pkg/profile"
	"github.com/skratchdot/open-golang/open"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/helpers/graph"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/logs"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/outputs/eager"
	"github.com/cube2222/octosql/outputs/formats"
	"github.com/cube2222/octosql/outputs/stream"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/table_valued_functions"
)

var VERSION = "dev"

var emptyYamlNode = func() yaml.Node {
	var out yaml.Node
	if err := yaml.Unmarshal([]byte("{}"), &out); err != nil {
		log.Fatalf("[BUG] Couldn't create empty yaml node: %s", err)
	}
	return out
}()

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "octosql <query>",
	Args:  cobra.ExactArgs(1),
	Short: "",
	Long:  ``,
	Example: `octosql "SELECT * FROM myfile.json"
octosql "SELECT * FROM mydir/myfile.csv"
octosql "SELECT * FROM plugins.plugins"`,
	SilenceErrors: true,
	Version:       VERSION,
	RunE: func(cmd *cobra.Command, args []string) error {
		switch prof {
		case "cpu":
			defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
		case "memory":
			defer profile.Start(profile.MemProfile, profile.MemProfileRate(4096*8), profile.ProfilePath(".")).Stop()
		case "trace":
			defer profile.Start(profile.TraceProfile, profile.ProfilePath(".")).Stop()
		}
		ctx := cmd.Context()
		debug.SetGCPercent(1000)

		logs.InitializeFileLogger()
		defer logs.CloseLogger()

		// TODO: Fixme
		// 	pluginManager := &manager.PluginManager{}
		//
		// 	pluginExecutor := executor.PluginExecutor{
		// 		Manager: pluginManager,
		// 	}
		// 	defer func() {
		// 		if err := pluginExecutor.Close(); err != nil {
		// 			log.Printf("couldn't close plugin executor: %s", err)
		// 		}
		// 	}()
		//
		// 	cfg, err := config.Read()
		// 	if err != nil {
		// 		return fmt.Errorf("couldn't read config: %w", err)
		// 	}
		//
		// 	installedPlugins, err := pluginManager.ListInstalledPlugins()
		// 	if err != nil {
		// 		return fmt.Errorf("couldn't list installed plugins: %w", err)
		// 	}
		//
		// 	resolvedVersions := map[string]*semver.Version{}
		//
		// 	// Fill in plugin versions.
		// dbLoop:
		// 	for i := range cfg.Databases {
		// 		if cfg.Databases[i].Version == nil {
		// 			constraint, _ := semver.NewConstraint("*")
		// 			cfg.Databases[i].Version = config.NewYamlUnmarshallableVersionConstraint(constraint)
		// 		}
		// 		for _, plugin := range installedPlugins {
		// 			if plugin.Reference != cfg.Databases[i].Type {
		// 				continue
		// 			}
		// 			for _, version := range plugin.Versions {
		// 				if cfg.Databases[i].Version.Raw().Check(version.Number) {
		// 					resolvedVersions[cfg.Databases[i].Name] = version.Number
		// 					continue dbLoop
		// 				}
		// 			}
		// 			break
		// 		}
		// 		return fmt.Errorf("database '%s' plugin '%s' used in configuration is not installed with the required version - run `octosql plugin install` to install all missing plugins", cfg.Databases[i].Name, cfg.Databases[i].Type.String())
		// 	}
		//
		databases := make(map[string]func() (physical.Database, error))
		// 	for _, dbConfig := range cfg.Databases {
		// 		once := sync.Once{}
		// 		curDbConfig := dbConfig
		// 		var db physical.Database
		// 		var err error
		//
		// 		databases[curDbConfig.Name] = func() (physical.Database, error) {
		// 			once.Do(func() {
		// 				db, err = pluginExecutor.RunPlugin(ctx, curDbConfig.Type, curDbConfig.Name, resolvedVersions[curDbConfig.Name], curDbConfig.Config)
		// 			})
		// 			if err != nil {
		// 				return nil, fmt.Errorf("couldn't run %s plugin %s: %w", curDbConfig.Type.String(), curDbConfig.Name, err)
		// 			}
		// 			return db, nil
		// 		}
		// 	}
		// TODO: Fixme
		// {
		// 	once := sync.Once{}
		// 	var repositories []repository.Repository
		// 	var err error
		// 	databases["plugins"] = func() (physical.Database, error) {
		// 		once.Do(func() {
		// 			repositories, err = repository.GetRepositories(ctx)
		// 		})
		// 		if err != nil {
		// 			return nil, fmt.Errorf("couldn't get repositories: %w", err)
		// 		}
		// 		return plugins.Creator(ctx, pluginManager, repositories)
		// 	}
		// }
		// TODO: Fixme
		// databases["docs"] = func() (physical.Database, error) {
		// 	return docs.Creator(ctx)
		// }
		// TODO: Fixme
		// for _, metadata := range installedPlugins {
		// 	if _, ok := databases[metadata.Reference.Name]; ok {
		// 		continue
		// 	}
		// 	curMetadata := metadata
		//
		// 	once := sync.Once{}
		// 	var db physical.Database
		// 	var err error
		//
		// 	databases[curMetadata.Reference.Name] = func() (physical.Database, error) {
		// 		once.Do(func() {
		// 			db, err = pluginExecutor.RunPlugin(ctx, curMetadata.Reference, curMetadata.Reference.Name, curMetadata.Versions[0].Number, emptyYamlNode)
		// 		})
		// 		if err != nil {
		// 			return nil, fmt.Errorf("couldn't run default plugin %s database: %w", curMetadata.Reference, err)
		// 		}
		// 		return db, nil
		// 	}
		// }

		// fileExtensionHandlers, err := pluginManager.GetFileExtensionHandlers()
		// if err != nil {
		// 	return fmt.Errorf("couldn't get file extension handlers: %w", err)
		// }
		fileHandlers := map[string]func(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error){
			// "csv":   csv.Creator(','), TODO: Fixme
			"json": json.Creator,
			// "lines": lines.Creator, TODO: Fixme
			// "parquet": parquet.Creator, TODO: Fixme
			// "tsv": csv.Creator('\t'), TODO: Fixme
		}
		// for ext, pluginName := range fileExtensionHandlers {
		// 	fileHandlers[ext] = func(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
		// 		db, err := databases[pluginName]()
		// 		if err != nil {
		// 			return nil, physical.Schema{}, fmt.Errorf("couldn't get plugin %s database for plugin extensions %s: %w", pluginName, ext, err)
		// 		}
		// 		return db.GetTable(ctx, name, options)
		// 	}
		// }
		for name := range fileHandlers {
			curHandler := fileHandlers[name]
			if _, ok := databases[name]; !ok {
				databases[name] = func() (physical.Database, error) {
					return &fileTypeDatabaseCreator{
						creator: curHandler,
					}, nil
				}
			}
		}

		env := physical.Environment{
			Aggregates: aggregates.Aggregates,
			Functions:  functions.FunctionMap(),
			Datasources: &physical.DatasourceRepository{
				Databases:    databases,
				FileHandlers: fileHandlers,
			},
			PhysicalConfig:  nil,
			VariableContext: nil,
		}
		statement, err := sqlparser.Parse(args[0])
		if err != nil {
			return fmt.Errorf("couldn't parse query: %w", err)
		}
		selectStmt, ok := statement.(sqlparser.SelectStatement)
		if !ok {
			return fmt.Errorf("only SELECT statements are supported")
		}
		logicalPlan, outputOptions, err := parser.ParseNode(selectStmt)
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

		physicalOrderByExpressions := make([]physical.Expression, len(outputOptions.OrderByExpressions))
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
			physicalOrderByExpressions[i] = physicalExpr
		}
		var physicalLimitExpression *physical.Expression
		if outputOptions.Limit != nil {
			physicalExpr, err := typecheckExpr(ctx, *outputOptions.Limit, env.WithRecordSchema(physicalPlan.Schema), logical.Environment{
				CommonTableExpressions: map[string]logical.CommonTableExpression{},
				TableValuedFunctions:   tableValuedFunctions,
				UniqueVariableNames: &logical.VariableMapping{
					Mapping: mapping,
				},
				UniqueNameGenerator: uniqueNameGenerator,
			})
			if err != nil {
				return fmt.Errorf("couldn't typecheck limit expression with index: %w", err)
			}
			physicalLimitExpression = &physicalExpr
		}

		// queryTelemetry := telemetry.GetQueryTelemetryData(physicalPlan, installedPlugins)

		var executionPlan execution.Node
		var orderByExpressions []execution.Expression
		var limitExpression *execution.Expression
		var outSchema physical.Schema
		if describe {
			// telemetry.SendTelemetry(ctx, VERSION, "describe", queryTelemetry)

			for i := range physicalPlan.Schema.Fields {
				physicalPlan.Schema.Fields[i].Name = reverseMapping[physicalPlan.Schema.Fields[i].Name]
			}
			executionPlan = &DescribeNode{
				Schema: physical.Schema{
					Fields:        formats.WithoutQualifiers(physicalPlan.Schema.Fields),
					TimeField:     physicalPlan.Schema.TimeField,
					NoRetractions: physicalPlan.Schema.NoRetractions,
				},
			}
			outSchema = DescribeNodeSchema
		} else {
			// telemetry.SendTelemetry(ctx, VERSION, "query", queryTelemetry)

			if optimize {
				physicalPlan = optimizer.Optimize(physicalPlan)
			}

			if explain >= 1 {
				file, err := os.CreateTemp(os.TempDir(), "octosql-explain-*.png")
				if err != nil {
					return fmt.Errorf("couldn't create temporary file: %w", err)
				}
				cmd := exec.Command("dot", "-Tpng")
				cmd.Stdin = strings.NewReader(graph.Show(physical.ExplainNode(physicalPlan, explain >= 2)).String())
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
				return nil
			}

			executionPlan, err = physicalPlan.Materialize(
				ctx,
				env,
			)
			if err != nil {
				return fmt.Errorf("couldn't materialize physical plan: %w", err)
			}

			orderByExpressions = make([]execution.Expression, len(physicalOrderByExpressions))
			for i, physicalExpr := range physicalOrderByExpressions {
				execExpr, err := physicalExpr.Materialize(ctx, env.WithRecordSchema(physicalPlan.Schema))
				if err != nil {
					return fmt.Errorf("couldn't materialize output order by expression with index %d: %v", i, err)
				}
				orderByExpressions[i] = execExpr
			}
			if physicalLimitExpression != nil {
				execExpr, err := physicalLimitExpression.Materialize(ctx, env.WithRecordSchema(physicalPlan.Schema))
				if err != nil {
					return fmt.Errorf("couldn't materialize output limit expression with index: %w", err)
				}
				limitExpression = &execExpr
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

		execCtx := execution.ExecutionContext{
			Context:         ctx,
			VariableContext: nil,
			CurrentRecords: execution.RecordBatch{
				Size: 1, // TODO: Fix this hack.
			},
		}

		switch output {
		case "live_table", "batch_table":
			panic("implement me")
			// var limit *int
			// if limitExpression != nil {
			// 	val, err := (*limitExpression).Evaluate(execCtx)
			// 	if err != nil {
			// 		return fmt.Errorf("couldn't evaluate limit expression: %w", err)
			// 	}
			// 	if val.Int < 0 {
			// 		return fmt.Errorf("limit must be positive, got %d", val.Int)
			// 	}
			// 	limit = &val.Int
			//
			// 	if len(orderByExpressions) == 0 && physicalPlan.Schema.NoRetractions {
			// 		// We want short-circuiting.
			// 		executionPlan = nodes.NewLimit(executionPlan, *limitExpression)
			// 	}
			// }
			//
			// sink = batch.NewOutputPrinter(
			// 	executionPlan,
			// 	orderByExpressions,
			// 	logical.DirectionsToMultipliers(outputOptions.OrderByDirections),
			// 	limit,
			// 	physicalPlan.Schema.NoRetractions,
			// 	outSchema,
			// 	func(writer io.Writer) batch.Format {
			// 		return formats.NewTableFormatter(writer)
			// 	},
			// 	output == "live_table",
			// )
		case "csv", "json":
			if len(orderByExpressions) > 0 || (limitExpression != nil && !physicalPlan.Schema.NoRetractions) {
				executionPlan = nodes.NewOrderSensitiveTransform(executionPlan, orderByExpressions, logical.DirectionsToMultipliers(outputOptions.OrderByDirections), limitExpression, physicalPlan.Schema.NoRetractions)
			} else if limitExpression != nil {
				executionPlan = nodes.NewLimit(executionPlan, *limitExpression)
			}

			var formatter func(writer io.Writer) eager.Format
			switch output {
			case "csv":
				formatter = func(writer io.Writer) eager.Format {
					return formats.NewCSVFormatter(writer)
				}
			case "json":
				formatter = func(writer io.Writer) eager.Format {
					return formats.NewJSONFormatter(writer)
				}
			}

			sink = eager.NewOutputPrinter(
				executionPlan,
				outSchema,
				formatter,
			)

		case "stream_native":
			if len(orderByExpressions) > 0 || (limitExpression != nil && !physicalPlan.Schema.NoRetractions) {
				executionPlan = nodes.NewOrderSensitiveTransform(executionPlan, orderByExpressions, logical.DirectionsToMultipliers(outputOptions.OrderByDirections), limitExpression, physicalPlan.Schema.NoRetractions)
			} else if limitExpression != nil {
				executionPlan = nodes.NewLimit(executionPlan, *limitExpression)
			}

			sink = stream.NewOutputPrinter(
				executionPlan,
				stream.NewNativeFormat(outSchema),
			)
		default:
			return fmt.Errorf("invalid output format: '%s'", output)
		}

		trace.Log(ctx, "octosql", "running query")
		if err := sink.Run(execCtx); err != nil {
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
var output string
var prof string

func init() {
	rootCmd.Flags().BoolVar(&describe, "describe", false, "Describe query output schema.")
	rootCmd.Flags().IntVar(&explain, "explain", 0, "Describe query output schema.")
	rootCmd.Flags().BoolVar(&optimize, "optimize", true, "Whether OctoSQL should optimize the query.")
	rootCmd.Flags().StringVarP(&output, "output", "o", "live_table", "Output format to use. Available options are live_table, batch_table, csv, json and stream_native.")
	rootCmd.Flags().StringVar(&prof, "profile", "", "Enable profiling of the given type: cpu, memory, trace.")
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

type fileTypeDatabaseCreator struct {
	creator func(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error)
}

func (f *fileTypeDatabaseCreator) ListTables(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (f *fileTypeDatabaseCreator) GetTable(ctx context.Context, name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	return f.creator(name, options)
}

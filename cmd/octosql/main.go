package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"runtime"
	"runtime/debug"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/go-chi/chi"

	"github.com/go-chi/chi/middleware"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/excel"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/kafka"
	"github.com/cube2222/octosql/datasources/parquet"
	"github.com/cube2222/octosql/datasources/redis"
	"github.com/cube2222/octosql/datasources/sql/mysql"
	"github.com/cube2222/octosql/datasources/sql/postgres"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/output/batch"
	batchcsv "github.com/cube2222/octosql/output/batch/csv"
	batchtable "github.com/cube2222/octosql/output/batch/table"
	"github.com/cube2222/octosql/output/streaming"
	streamingjson "github.com/cube2222/octosql/output/streaming/json"

	"github.com/spf13/cobra"

	"github.com/cube2222/octosql/app"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

var version string
var configPath string
var outputFormat string
var storageDirectory string
var storageInMemory bool
var logFilePath string
var describe bool

var rootCmd = &cobra.Command{
	Use:   "octosql <query>",
	Short: "OctoSQL is a data querying tool, allowing you to join, analyze and transform data from multiple data sources and file formats using SQL.",
	Long: `OctoSQL is a SQL query engine which allows you to write standard SQL queries on data stored in multiple SQL databases, NoSQL databases and files in various formats trying to push down as much of the work as possible to the source databases, not transferring unnecessary data.

OctoSQL does that by creating an internal representation of your query and later translating parts of it into the query languages or APIs of the source databases. Whenever a datasource doesn't support a given operation, OctoSQL will execute it in memory, so you don't have to worry about the specifics of the underlying datasources.

With OctoSQL you don't need O(n) client tools or a large data analysis system deployment. Everything's contained in a single binary.`,
	Args: cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		query := args[0]

		// Configuration
		cfg, err := config.ReadConfig(configPath)
		if err != nil {
			log.Fatal(err)
		}
		dataSourceRespository, err := physical.CreateDataSourceRepositoryFromConfig(
			map[string]physical.Factory{
				"csv":      csv.NewDataSourceBuilderFactoryFromConfig,
				"json":     json.NewDataSourceBuilderFactoryFromConfig,
				"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
				"postgres": postgres.NewDataSourceBuilderFactoryFromConfig,
				"redis":    redis.NewDataSourceBuilderFactoryFromConfig,
				"excel":    excel.NewDataSourceBuilderFactoryFromConfig,
				"kafka":    kafka.NewDataSourceBuilderFactoryFromConfig,
				"parquet":  parquet.NewDataSourceBuilderFactoryFromConfig,
			},
			cfg,
		)
		if err != nil {
			log.Fatal(err)
		}

		var streamingMode bool
		var outputSinkFn app.OutputSinkFn
		switch outputFormat {
		case "stream-json":
			streamingMode = true
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName, outputOptions *app.OutputOptions) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := streaming.NewInstantStreamOutput(streamID)
				output := streaming.NewStreamPrinter(
					stateStorage,
					sink,
					streamingjson.JSONPrinter(),
				)
				return sink, output
			}
		case "live-csv":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName, outputOptions *app.OutputOptions) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField, outputOptions.OrderByExpressions, outputOptions.OrderByDirections, outputOptions.Limit, outputOptions.Offset)
				output := batch.NewLiveTablePrinter(stateStorage, sink, batchcsv.TableFormatter(','))
				return sink, output
			}
		case "live-table":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName, outputOptions *app.OutputOptions) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField, outputOptions.OrderByExpressions, outputOptions.OrderByDirections, outputOptions.Limit, outputOptions.Offset)
				output := batch.NewLiveTablePrinter(stateStorage, sink, batchtable.TableFormatter(false))
				return sink, output
			}
		case "batch-csv":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName, outputOptions *app.OutputOptions) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField, outputOptions.OrderByExpressions, outputOptions.OrderByDirections, outputOptions.Limit, outputOptions.Offset)
				output := batch.NewWholeTablePrinter(stateStorage, sink, batchcsv.TableFormatter(','))
				return sink, output
			}
		case "batch-table":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName, outputOptions *app.OutputOptions) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField, outputOptions.OrderByExpressions, outputOptions.OrderByDirections, outputOptions.Limit, outputOptions.Offset)
				output := batch.NewWholeTablePrinter(stateStorage, sink, batchtable.TableFormatter(false))
				return sink, output
			}
		default:
			log.Fatal("invalid output type")
		}

		telemetryInfo := app.TelemetryInfo{
			OutputFormat: outputFormat,
			Version:      version,
		}

		app := app.NewApp(cfg, telemetryInfo, dataSourceRespository, outputSinkFn, describe)

		// Parse query
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			log.Fatal("couldn't parse query: ", err)
		}
		typed, ok := stmt.(sqlparser.SelectStatement)
		if !ok {
			log.Fatalf("invalid statement type, wanted sqlparser.SelectStatement got %v", reflect.TypeOf(stmt))
		}
		plan, outputOptions, err := parser.ParseNode(typed)
		if err != nil {
			log.Fatal("couldn't parse query: ", err)
		}
		if streamingMode {
			plan = logical.NewOrderBy(outputOptions.OrderByExpressions, outputOptions.OrderByDirections, plan)
		}

		if storageDirectory == "" {
			tempDir, err := ioutil.TempDir("", "octosql")
			if err != nil {
				log.Fatal("couldn't create temporary directory: ", err)
			}
			storageDirectory = tempDir
		}
		if err := os.MkdirAll(storageDirectory, os.ModePerm); err != nil {
			log.Fatal("couldn't create storage directory")
		}
		defer func() {
			err = os.RemoveAll(storageDirectory)
			if err != nil {
				log.SetOutput(os.Stderr)
				log.Fatal("couldn't remove temporary directory: ", err)
			}
		}()

		if logFilePath == "" {
			logFilePath = path.Join(storageDirectory, "octosql.log")
		}
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
		if err != nil {
			log.Fatalf("couldn't open file for logs %s: %s", logFilePath, err)
		}
		defer logFile.Close()
		log.SetOutput(logFile)
		log.Printf("Starting OctoSQL...")
		log.Printf("Using Configuration: %+v", *cfg)

		opts := badger.DefaultOptions(storageDirectory)
		opts.Logger = badgerLogger{}
		if runtime.GOOS == "windows" { // TODO - fix while refactoring config
			opts = opts.WithValueLogLoadingMode(options.FileIO)
		}
		if storageInMemory {
			opts = opts.
				WithInMemory(true).
				WithDir("").
				WithValueDir("")
		}

		db, err := badger.Open(opts)
		if err != nil {
			log.Fatal("couldn't open badger database: ", err)
		}
		defer func() {
			err = db.Close()
			if err != nil {
				log.Fatal("couldn't close the database: ", err)
			}
		}()

		stateStorage := storage.NewBadgerStorage(db)

		// Run query
		err = app.RunPlan(ctx, stateStorage, plan, outputOptions)
		if err != nil {
			log.Fatal("couldn't run plan: ", err)
		}
	},
}

func main() {
	info, ok := debug.ReadBuildInfo()
	if ok {
		version = info.Main.Version
	} else {
		version = "unknown"
	}
	rootCmd.SetVersionTemplate(fmt.Sprintf("OctoSQL Version: %s\n", version))
	rootCmd.Version = version

	rootCmd.Flags().StringVarP(&configPath, "config", "c", os.Getenv("OCTOSQL_CONFIG"), "data source configuration path, defaults to $OCTOSQL_CONFIG")
	rootCmd.Flags().StringVarP(&outputFormat, "output", "o", "live-table", "output format, one of [stream-json live-csv live-table batch-csv batch-table]")
	rootCmd.Flags().StringVar(&storageDirectory, "storage-directory", "", "directory to store state storage in")
	rootCmd.Flags().BoolVar(&storageInMemory, "storage-in-memory", false, "EXPERIMENTAL: Use badger in-memory mode for storage.")
	rootCmd.Flags().StringVar(&logFilePath, "log-file", "", "Logs output file, will append if the file exists.")
	rootCmd.Flags().BoolVar(&describe, "describe", false, "Print out the physical query plan in graphviz format. You can use a command like \"dot -Tpng file > output.png\" to view it.")

	go func() {
		r := chi.NewRouter()
		r.Mount("/debug", middleware.Profiler())
		log.Fatal(http.ListenAndServe("127.0.0.1:3001", r))
	}()

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

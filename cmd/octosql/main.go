package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"

	"github.com/dgraph-io/badger/v2"
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

var configPath string
var outputFormat string
var storageDirectory string
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

		var outputSinkFn app.OutputSinkFn
		switch outputFormat {
		case "stream-json":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := streaming.NewInstantStreamOutput(streamID)
				output := streaming.NewStreamPrinter(
					stateStorage,
					sink,
					streamingjson.JSONPrinter(),
				)
				return sink, output
			}
		case "live-csv":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField)
				out := batch.NewLiveTablePrinter(stateStorage, sink, batchcsv.TableFormatter(','))
				return sink, out
			}
		case "live-table":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField)
				out := batch.NewLiveTablePrinter(stateStorage, sink, batchtable.TableFormatter(false))
				return sink, out
			}
		case "batch-csv":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField)
				out := batch.NewWholeTablePrinter(stateStorage, sink, batchcsv.TableFormatter(','))
				return sink, out
			}
		case "batch-table":
			outputSinkFn = func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName) (store execution.IntermediateRecordStore, printer output.Printer) {
				sink := batch.NewTableOutput(streamID, eventTimeField)
				out := batch.NewWholeTablePrinter(stateStorage, sink, batchtable.TableFormatter(false))
				return sink, out
			}
		default:
			log.Fatal("invalid output type")
		}

		app := app.NewApp(cfg, dataSourceRespository, outputSinkFn, describe)

		// Parse query
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			log.Fatal("couldn't parse query: ", err)
		}
		typed, ok := stmt.(sqlparser.SelectStatement)
		if !ok {
			log.Fatalf("invalid statement type, wanted sqlparser.SelectStatement got %v", reflect.TypeOf(stmt))
		}
		plan, err := parser.ParseNode(typed)
		if err != nil {
			log.Fatal("couldn't parse query: ", err)
		}

		if storageDirectory == "" {
			tempDir, err := ioutil.TempDir("", "octosql")
			if err != nil {
				log.Fatal("couldn't create temporary directory: ", err)
			}
			storageDirectory = tempDir
		}

		log.Println(storageDirectory)
		opts := badger.DefaultOptions(storageDirectory)
		db, err := badger.Open(opts)
		if err != nil {
			log.Fatal("couldn't open badger database: ", err)
		}

		stateStorage := storage.NewBadgerStorage(db)

		// Run query
		err = app.RunPlan(ctx, stateStorage, plan)
		if err != nil {
			log.Fatal("couldn't run plan: ", err)
		}

		err = db.Close()
		if err != nil {
			log.Fatal("couldn't close the database: ", err)
		}

		err = os.RemoveAll(storageDirectory)
		if err != nil {
			log.Fatal("couldn't remove temporary directory: ", err)
		}
	},
}

func main() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", os.Getenv("OCTOSQL_CONFIG"), "data source configuration path, defaults to $OCTOSQL_CONFIG")
	rootCmd.Flags().StringVarP(&outputFormat, "output", "o", "live-table", "output format, one of [table json csv tabbed table_row_separated]")
	rootCmd.Flags().StringVar(&storageDirectory, "storage-directory", "", "directory to store state storage in")
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

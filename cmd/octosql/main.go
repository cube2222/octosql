package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"

	"github.com/dgraph-io/badger/v2"
	"github.com/go-chi/chi"

	"github.com/go-chi/chi/middleware"

	"github.com/cube2222/octosql/storage/excel"
	"github.com/cube2222/octosql/storage/kafka"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/spf13/cobra"

	"github.com/cube2222/octosql/app"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/output"
	csvoutput "github.com/cube2222/octosql/output/csv"
	jsonoutput "github.com/cube2222/octosql/output/json"
	"github.com/cube2222/octosql/output/table"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/mysql"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/cube2222/octosql/storage/redis"
)

var configPath string
var outputFormat string
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
			},
			cfg,
		)
		if err != nil {
			log.Fatal(err)
		}

		var out output.Output
		switch outputFormat {
		case "table":
			out = table.NewOutput(os.Stdout, false)
		case "table_row_separated":
			out = table.NewOutput(os.Stdout, true)
		case "json":
			out = jsonoutput.NewOutput(os.Stdout)
		case "csv":
			out = csvoutput.NewOutput(',', os.Stdout)
		case "tabbed":
			out = csvoutput.NewOutput('\t', os.Stdout)
		default:
			log.Fatal("invalid output type")
		}

		app := app.NewApp(cfg, dataSourceRespository, out, describe)

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

		opts := badger.DefaultOptions("")
		opts.Dir = ""
		opts.ValueDir = ""
		opts.InMemory = true
		db, err := badger.Open(opts)
		if err != nil {
			log.Fatal("couldn't open in-memory badger database: ", err)
		}
		stateStorage := storage.NewBadgerStorage(db)

		// Run query
		err = app.RunPlan(ctx, stateStorage, plan)
		if err != nil {
			log.Fatal("couldn't run plan: ", err)
		}
	},
}

func main() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", os.Getenv("OCTOSQL_CONFIG"), "data source configuration path, defaults to $OCTOSQL_CONFIG")
	rootCmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "output format, one of [table json csv tabbed table_row_separated]")
	rootCmd.Flags().BoolVar(&describe, "describe", false, "Print out the physical query plan in graphviz format. You can use a command like \"dot -Tpng file > output.png\" to view it.")

	go func() {
		r := chi.NewRouter()
		r.Mount("/debug", middleware.Profiler())
		log.Fatal(http.ListenAndServe(":3003", r))
	}()

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

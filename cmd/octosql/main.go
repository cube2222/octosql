package main

import (
	"context"
	"fmt"
	"github.com/cube2222/octosql/octosql"
	"github.com/go-chi/chi"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/middleware"

	"github.com/spf13/cobra"
)

var configPath string
var outputFormat string

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

		executor, err := octosql.NewOctosqlExecutor()
		if err != nil {
			log.Fatal(err)
		}

		err = executor.LoadConfiguration(configPath)
		if err != nil {
			log.Fatal(err)
		}

		err = executor.RunQuery(ctx, query)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func main() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", os.Getenv("OCTOSQL_CONFIG"), "data source configuration path, defaults to $OCTOSQL_CONFIG")
	rootCmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "output format, one of [table json csv tabbed table_row_separated]")

	go func() {
		r := chi.NewRouter()
		r.Mount("/debug", middleware.Profiler())
		log.Fatal(http.ListenAndServe(":3000", r))
	}()

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

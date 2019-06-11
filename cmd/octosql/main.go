package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/cube2222/octosql/app"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/output"
	jsonoutput "github.com/cube2222/octosql/output/json"
	"github.com/cube2222/octosql/output/table"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/mysql"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/cube2222/octosql/storage/redis"
	"github.com/spf13/cobra"
	"github.com/xwb1989/sqlparser"
)

var configPath string
var outputFormat string

var rootCmd = &cobra.Command{
	Use:   "octosql <query>",
	Short: "A brief description of your application.", //TODO: write short description
	Long:  `A long description of your application.`,  //TODO: write long description
	Args:  cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		query := args[0]

		// Configuration
		cfg, err := config.ReadConfig(configPath)
		if err != nil {
			log.Fatal(err)
		}
		dataSourceRespository, err := config.CreateDataSourceRepositoryFromConfig(
			map[string]config.Factory{
				"csv":      csv.NewDataSourceBuilderFactoryFromConfig,
				"json":     json.NewDataSourceBuilderFactoryFromConfig,
				"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
				"postgres": postgres.NewDataSourceBuilderFactoryFromConfig,
				"redis":    redis.NewDataSourceBuilderFactoryFromConfig,
			},
			cfg,
		)
		if err != nil {
			log.Fatal(err)
		}

		var out output.Output
		switch outputFormat {
		case "table":
			out = table.NewOutput(os.Stdout)
		case "json":
			out = jsonoutput.NewOutput(os.Stdout)
		default:
			log.Fatal("invalid output type")
		}

		app := app.NewApp(dataSourceRespository, out)

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

		// Run query
		err = app.RunPlan(ctx, plan)
		if err != nil {
			log.Fatal("couldn't run plan: ", err)
		}
	},
}

func main() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", os.Getenv("OCTOSQL_CONFIG"), "data source configuration path, defaults to $OCTOSQL_CONFIG")
	rootCmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "output format, one of [table json csv]")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

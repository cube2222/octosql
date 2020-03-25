package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/excel"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/redis"
	"github.com/cube2222/octosql/storage/sql/mysql"
	"github.com/cube2222/octosql/storage/sql/postgres"
	"github.com/spf13/cobra"
)

var target string
var dbType string

var logicalViz = &cobra.Command{
	Use:   "sqlviz <query>",
	Short: ".",
	Long:  `.`,
	Args:  cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		query := args[0]

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

		if target == "" || target == "logical" {
			fmt.Println(graph.Show(plan.Visualize()))
			return
		}

		var dsBuilderFactory physical.DataSourceBuilderFactory
		switch dbType {
		case "csv":
			dsBuilderFactory = csv.NewDataSourceBuilderFactory()
		case "json":
			dsBuilderFactory = json.NewDataSourceBuilderFactory()
		case "mysql":
			dsBuilderFactory = mysql.NewDataSourceBuilderFactory(nil)
		case "postgres":
			dsBuilderFactory = postgres.NewDataSourceBuilderFactory(nil)
		case "redis":
			dsBuilderFactory = redis.NewDataSourceBuilderFactory("graphviz")
		case "excel":
			dsBuilderFactory = excel.NewDataSourceBuilderFactory()
		default:
			dsBuilderFactory = mysql.NewDataSourceBuilderFactory(nil)
		}

		dataSourceRespository := dataSourceRepository{dsBuilderFactory}

		phys, _, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(dataSourceRespository))
		if err != nil {
			log.Fatal("couldn't create physical plan:", err)
		}

		if target == "physical" {
			fmt.Println(graph.Show(phys.Visualize()))
			return
		}

		phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

		fmt.Println(graph.Show(phys.Visualize()))
		return
	},
}

type dataSourceRepository struct {
	dsBuilderFactory physical.DataSourceBuilderFactory
}

// Get gets a new builder for a given data source.
func (repo dataSourceRepository) Get(dataSourceName, alias string) (*physical.DataSourceBuilder, error) {
	return repo.dsBuilderFactory(dataSourceName, alias), nil
}

func main() {
	logicalViz.Flags().StringVar(&dbType, "db", "mysql", "the datasource type.")
	logicalViz.Flags().StringVar(&target, "target", "logical", "Print out the logical/physical/optimizer query plan in graphviz format. You can use a command like \"dot -Tpng file > output.png\" to view it.")

	if err := logicalViz.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

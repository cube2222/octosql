package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bradleyjkemp/memmap"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/cube2222/octosql/storage/redis"
	"github.com/xwb1989/sqlparser"
)

func main() {
	ctx := context.Background()
	query := os.Args[1]

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	typed, ok := stmt.(sqlparser.SelectStatement)
	if !ok {
		log.Fatal("invalid statement type")
	}
	parsed, err := parser.ParseNode(typed)
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := config.ReadConfig(os.Getenv("OCTOSQL_CONFIG"))
	if err != nil {
		log.Fatal(err)
	}
	dataSourceRespository, err := config.CreateDataSourceRepositoryFromConfig(
		map[string]config.Factory{
			"csv":      csv.NewDataSourceBuilderFactoryFromConfig,
			"json":     json.NewDataSourceBuilderFactoryFromConfig,
			"postgres": postgres.NewDataSourceBuilderFactoryFromConfig,
			"redis":    redis.NewDataSourceBuilderFactoryFromConfig,
		},
		cfg,
	)
	if err != nil {
		log.Fatal(err)
	}

	phys, variables, err := parsed.Physical(ctx, logical.NewPhysicalPlanCreator(dataSourceRespository))
	if err != nil {
		log.Fatal(err)
	}

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	f, err := os.Create("diag_got")
	if err != nil {
		log.Fatal(err)
	}
	memmap.Map(f, phys)
	f.Close()

	exec, err := phys.Materialize(ctx)
	if err != nil {
		log.Fatal(err)
	}

	stream, err := exec.Get(variables)
	if err != nil {
		log.Fatal(err)
	}

	var rec *execution.Record
	for rec, err = stream.Next(); err == nil; rec, err = stream.Next() {
		var out []string
		fields := rec.Fields()
		for i := range fields {
			v := rec.Value(fields[i].Name)
			out = append(out, fmt.Sprintf("%v: %v", fields[i].Name, v))
		}
		fmt.Println(strings.Join(out, ", "))
	}
	if err != execution.ErrEndOfStream {
		log.Fatal(err)
	}
}

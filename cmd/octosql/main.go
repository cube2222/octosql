package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bradleyjkemp/memmap"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/mysql"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/cube2222/octosql/storage/redis"
	"github.com/olekukonko/tablewriter"
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
			"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
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

	exec, err := phys.Materialize(ctx)
	if err != nil {
		log.Fatal(err)
	}

	stream, err := exec.Get(variables)
	if err != nil {
		log.Fatal(err)
	}

	var rec *execution.Record
	var records []*execution.Record
	for rec, err = stream.Next(); err == nil; rec, err = stream.Next() {
		records = append(records, rec)
	}
	if err != execution.ErrEndOfStream {
		log.Fatal(err)
	}

	var fields []string
	for _, record := range records {
		for _, field := range record.Fields() {
			found := false
			for i := range fields {
				if fields[i] == field.Name.String() {
					found = true
				}
			}
			if !found {
				fields = append(fields, field.Name.String())
			}
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(fields)

	for _, record := range records {
		var out []string
		for _, field := range fields {
			value := record.Value(octosql.NewVariableName(field))
			out = append(out, fmt.Sprint(value))
		}
		table.Append(out)
	}

	table.Render()
}

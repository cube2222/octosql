package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/xwb1989/sqlparser"
)

func main() {
	ctx := context.Background()
	query := os.Args[1]

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	typed, ok := stmt.(*sqlparser.Select)
	if !ok {
		log.Fatal("invalid statement type")
	}
	parsed, err := parser.ParseNode(typed)
	if err != nil {
		log.Fatal(err)
	}

	dataSourceRespository := physical.NewDataSourceRepository()
	err = dataSourceRespository.Register("people", json.NewDataSourceBuilderFactory("people.json"))
	if err != nil {
		log.Fatal(err)
	}
	err = dataSourceRespository.Register("cities", json.NewDataSourceBuilderFactory("cities.json"))
	if err != nil {
		log.Fatal(err)
	}

	err = dataSourceRespository.Register("users",
		postgres.NewDataSourceBuilderFactory("localhost", "postgres",
			"", "mydb", "users", nil, 5432))

	if err != nil {
		log.Fatal(err)
	}

	phys, variables, err := parsed.Physical(ctx, logical.NewPhysicalPlanCreator(dataSourceRespository))
	if err != nil {
		log.Fatal(err)
	}

	physTyped := phys.(*physical.Filter)

	aliases := postgres.NewAliases("u")

	translated, err := postgres.FormulaToSQL(physTyped.Formula, aliases)

	log.Println(translated)

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
	for rec, err = stream.Next(); err == nil; rec, err = stream.Next() {
		var out []string
		fields := rec.Fields()
		for i := range fields {
			out = append(out, fmt.Sprintf("%v: %v", fields[i].Name, rec.Value(fields[i].Name)))
		}
		fmt.Println(strings.Join(out, ", "))
	}
	if err != execution.ErrEndOfStream {
		log.Fatal(err)
	}
}

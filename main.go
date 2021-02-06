package main

import (
	"context"
	"log"
	"os"

	"github.com/davecgh/go-spew/spew"

	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
)

func main() {
	statement, err := sqlparser.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	logicalPlan, err := parser.ParseSelect(statement.(*sqlparser.Select))
	if err != nil {
		log.Fatal(err)
	}
	spew.Dump(logicalPlan)
	// TODO: Wrap panics into errors in subfunction.
	physicalPlan := logicalPlan.Typecheck(
		context.Background(),
		physical.Environment{
			Aggregates: nil,
			Datasources: &physical.DatasourceRepository{
				Datasources: map[string]func(name string) (physical.DatasourceImplementation, error){
					"json": json.Creator,
				},
			},
			PhysicalConfig:  nil,
			VariableContext: nil,
		},
		physical.State{},
	)
	spew.Dump(physicalPlan)
	spew.Dump(physicalPlan.Schema)
}

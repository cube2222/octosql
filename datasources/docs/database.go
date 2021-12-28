package docs

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(ctx context.Context) (physical.Database, error) {
	return &Database{}, nil
}

type Database struct {
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	return []string{
		"functions",
	}, nil
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	switch name {
	case "aggregates":
		return &aggregatesPhysical{},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "name",
						Type: octosql.String,
					},
					{
						Name: "description",
						Type: octosql.String,
					},
				},
			},
			nil
	case "aggregate_signatures":
		return &aggregateSignaturesPhysical{},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "name",
						Type: octosql.String,
					},
					{
						Name: "argument_type",
						Type: octosql.String,
					},
					{
						Name: "output_type",
						Type: octosql.String,
					},
					{
						Name: "simple_signature",
						Type: octosql.Boolean,
					},
				},
			},
			nil
	case "functions":
		return &functionsPhysical{},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "name",
						Type: octosql.String,
					},
					{
						Name: "description",
						Type: octosql.String,
					},
				},
			},
			nil
	case "function_signatures":
		return &functionSignaturesPhysical{},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "name",
						Type: octosql.String,
					},
					{
						Name: "argument_types",
						Type: octosql.Type{
							TypeID: octosql.TypeIDList,
							List:   struct{ Element *octosql.Type }{Element: &octosql.String},
						},
					},
					{
						Name: "output_type",
						Type: octosql.String,
					},
					{
						Name: "strict",
						Type: octosql.Boolean,
					},
					{
						Name: "simple_signature",
						Type: octosql.Boolean,
					},
				},
			},
			nil
	}
	return nil, physical.Schema{}, fmt.Errorf("unknown table: %s", name)
}

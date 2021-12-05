package main

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/plugin"
)

func Creator(ctx context.Context, configUntyped plugin.ConfigDecoder) (physical.Database, error) {
	return &Database{}, nil
}

type Database struct {
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	switch name {
	case "processes":
		return &processesPhysical{},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "pid",
						Type: octosql.Int,
					},
					{
						Name: "ppid",
						Type: octosql.Int,
					},
					{
						Name: "name",
						Type: octosql.String,
					},
					{
						Name: "create_time",
						Type: octosql.Time,
					},
					{
						Name: "user",
						Type: octosql.String,
					},
					{
						Name: "status",
						Type: octosql.String,
					},
					{
						Name: "rss",
						Type: octosql.Int,
					},
					{
						Name: "vms",
						Type: octosql.Int,
					},
				},
			},
			nil
	}
	return nil, physical.Schema{}, fmt.Errorf("unknown table: %s", name)
}

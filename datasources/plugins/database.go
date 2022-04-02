package plugins

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/plugins/manager"
	"github.com/cube2222/octosql/plugins/repository"

	"github.com/cube2222/octosql/physical"
)

func Creator(ctx context.Context, manager *manager.PluginManager, repos []repository.Repository) (physical.Database, error) {
	return &Database{
		Manager:      manager,
		Repositories: repos,
	}, nil
}

type Database struct {
	Manager      *manager.PluginManager
	Repositories []repository.Repository
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	return []string{
		"available_plugins",
		"repositories",
		"available_versions",
		"installed_plugins",
		"installed_versions",
	}, nil
}

func (d *Database) GetTable(ctx context.Context, name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	switch name {
	case "available_plugins":
		return &availablePluginsPhysical{
				repositories: d.Repositories,
			},
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
					{
						Name: "file_extensions",
						Type: octosql.Type{
							TypeID: octosql.TypeIDList,
							List:   struct{ Element *octosql.Type }{Element: &octosql.String},
						},
					},
					{
						Name: "website",
						Type: octosql.String,
					},
					{
						Name: "contact_email",
						Type: octosql.String,
					},
					{
						Name: "license",
						Type: octosql.String,
					},
					{
						Name: "readme_url",
						Type: octosql.String,
					},
					{
						Name: "repo_slug",
						Type: octosql.String,
					},
				},
			},
			nil
	case "repositories":
		return &repositoriesPhysical{
				repositories: d.Repositories,
			},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "slug",
						Type: octosql.String,
					},
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
	case "available_versions":
		return &availableVersionsPhysical{
				repositories: d.Repositories,
			},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "version",
						Type: octosql.String,
					},
					{
						Name: "prerelease",
						Type: octosql.Boolean,
					},
					{
						Name: "plugin_name",
						Type: octosql.String,
					},
					{
						Name: "repo_slug",
						Type: octosql.String,
					},
				},
			},
			nil
	case "installed_plugins":
		return &installedPluginsPhysical{
				manager: d.Manager,
			},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "name",
						Type: octosql.String,
					},
					{
						Name: "repo_slug",
						Type: octosql.String,
					},
				},
			},
			nil
	case "installed_versions":
		return &installedVersionsPhysical{
				manager: d.Manager,
			},
			physical.Schema{
				TimeField: -1,
				Fields: []physical.SchemaField{
					{
						Name: "version",
						Type: octosql.String,
					},
					{
						Name: "prerelease",
						Type: octosql.Boolean,
					},
					{
						Name: "plugin_name",
						Type: octosql.String,
					},
					{
						Name: "repo_slug",
						Type: octosql.String,
					},
				},
			},
			nil
	}
	return nil, physical.Schema{}, fmt.Errorf("unknown table: %s", name)
}

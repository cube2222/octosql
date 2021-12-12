package plugins

import (
	"context"
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/manager"
)

type installedVersionsPhysical struct {
	manager *manager.PluginManager
}

func (i *installedVersionsPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &installedVersionsExecuting{
		manager: i.manager,
		fields:  schema.Fields,
	}, nil
}

func (i *installedVersionsPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type installedVersionsExecuting struct {
	manager *manager.PluginManager
	fields  []physical.SchemaField
}

func (d *installedVersionsExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	installedPlugins, err := d.manager.ListInstalledPlugins()
	if err != nil {
		return fmt.Errorf("failed to list installed plugins: %w", err)
	}

	for _, plugin := range installedPlugins {
		for _, version := range plugin.Versions {
			values := make([]octosql.Value, len(d.fields))
			for i, field := range d.fields {
				switch field.Name {
				case "version":
					values[i] = octosql.NewString(version.Number.String())
				case "prerelease":
					values[i] = octosql.NewBoolean(version.Number.Prerelease() != "")
				case "plugin_name":
					values[i] = octosql.NewString(plugin.Reference.Name)
				case "repo_slug":
					values[i] = octosql.NewString(plugin.Reference.Repository)
				}
			}

			if err := produce(
				ProduceFromExecutionContext(ctx),
				NewRecord(values, false, time.Time{}),
			); err != nil {
				return fmt.Errorf("couldn't produce record: %w", err)
			}
		}
	}

	return nil
}

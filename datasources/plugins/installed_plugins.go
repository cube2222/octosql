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

type installedPluginsPhysical struct {
	manager *manager.PluginManager
}

func (i *installedPluginsPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &installedPluginsExecuting{
		manager: i.manager,
		fields:  schema.Fields,
	}, nil
}

func (i *installedPluginsPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type installedPluginsExecuting struct {
	manager *manager.PluginManager
	fields  []physical.SchemaField
}

func (d *installedPluginsExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	installedPlugins, err := d.manager.ListInstalledPlugins()
	if err != nil {
		return fmt.Errorf("failed to list installed plugins: %w", err)
	}

	for _, plugin := range installedPlugins {
		values := make([]octosql.Value, len(d.fields))
		for i, field := range d.fields {
			switch field.Name {
			case "name":
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

	return nil
}

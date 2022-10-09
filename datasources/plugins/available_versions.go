package plugins

import (
	"context"
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/repository"
)

type availableVersionsPhysical struct {
	repositories []repository.Repository
}

func (i *availableVersionsPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	if len(pushedDownPredicates) == 0 {
		return nil, fmt.Errorf("versions table requires you to filter by the plugin_name - either specify it directly or use a LOOKUP JOIN")
	}

	args := pushedDownPredicates[0].FunctionCall.Arguments

	var pluginNameExpression physical.Expression
	if args[0].ExpressionType == physical.ExpressionTypeVariable && args[0].Variable.Name == "plugin_name" {
		pluginNameExpression = args[1]
	} else {
		pluginNameExpression = args[0]
	}

	pluginNameExpressionExecuting, err := pluginNameExpression.Materialize(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("couldn't materialize plugin_name expression: %w", err)
	}

	return &availableVersionsExecuting{
		repositories:         i.repositories,
		fields:               schema.Fields,
		pluginNameExpression: pluginNameExpressionExecuting,
	}, nil
}

func (i *availableVersionsPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	if len(pushedDownPredicates) > 0 {
		return newPredicates, pushedDownPredicates, false
	}

	for i, pred := range newPredicates {
		if pred.ExpressionType != physical.ExpressionTypeFunctionCall {
			continue
		}
		if pred.FunctionCall.Name != "=" {
			continue
		}
		// One of the arguments must be the plugin_name.
		if args := pred.FunctionCall.Arguments; (args[0].ExpressionType != physical.ExpressionTypeVariable || args[0].Variable.Name != "plugin_name") &&
			(args[1].ExpressionType != physical.ExpressionTypeVariable || args[1].Variable.Name != "plugin_name") {
			continue
		}
		// The other one may not be top-level.
		if args := pred.FunctionCall.Arguments; args[0].ExpressionType == physical.ExpressionTypeVariable && args[0].Variable.IsLevel0 &&
			args[1].ExpressionType == physical.ExpressionTypeVariable && args[1].Variable.IsLevel0 {
			continue
		}

		return append(newPredicates[:i], newPredicates[i+1:]...), []physical.Expression{pred}, true
	}

	return newPredicates, pushedDownPredicates, false
}

type availableVersionsExecuting struct {
	repositories []repository.Repository
	fields       []physical.SchemaField

	pluginNameExpression Expression
}

func (d *availableVersionsExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	pluginName, err := d.pluginNameExpression.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate plugin_name expression: %w", err)
	}

	for _, repo := range d.repositories {
		for _, plugin := range repo.Plugins {
			if plugin.Name != pluginName.Str {
				continue
			}

			manifest, err := repository.GetManifest(ctx, plugin.ManifestURL)
			if err != nil {
				return fmt.Errorf("couldn't get manifest for plugin %s from %s: %w", plugin.Name, plugin.ManifestURL, err)
			}

			for _, version := range manifest.Versions {
				values := make([]octosql.Value, len(d.fields))
				for i, field := range d.fields {
					switch field.Name {
					case "version":
						values[i] = octosql.NewString(version.Number.String())
					case "prerelease":
						values[i] = octosql.NewBoolean(version.Number.Prerelease() != "")
					case "plugin_name":
						values[i] = octosql.NewString(plugin.Name)
					case "repo_slug":
						values[i] = octosql.NewString(repo.Slug)
					}
				}

				if err := produce(
					ProduceFromExecutionContext(ctx),
					NewRecordBatch(values, false, time.Time{}),
				); err != nil {
					return fmt.Errorf("couldn't produce record: %w", err)
				}
			}
		}
	}

	return nil
}

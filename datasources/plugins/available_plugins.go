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

type availablePluginsPhysical struct {
	repositories []repository.Repository
}

func (i *availablePluginsPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (Node, error) {
	return &availablePluginsExecuting{
		repositories: i.repositories,
		fields:       schema.Fields,
	}, nil
}

func (i *availablePluginsPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

type availablePluginsExecuting struct {
	repositories []repository.Repository
	fields       []physical.SchemaField
}

func (d *availablePluginsExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for _, repo := range d.repositories {
		for _, plugin := range repo.Plugins {
			values := make([]octosql.Value, len(d.fields))
			for i, field := range d.fields {
				switch field.Name {
				case "name":
					values[i] = octosql.NewString(plugin.Name)
				case "description":
					values[i] = octosql.NewString(plugin.Description)
				case "file_extensions":
					list := make([]octosql.Value, len(plugin.FileExtensions))
					for j, ext := range plugin.FileExtensions {
						list[j] = octosql.NewString(ext)
					}
					values[i] = octosql.NewList(list)
				case "website":
					values[i] = octosql.NewString(plugin.Website)
				case "contact_email":
					values[i] = octosql.NewString(plugin.ContactEmail)
				case "license":
					values[i] = octosql.NewString(plugin.License)
				case "readme_url":
					values[i] = octosql.NewString(plugin.ReadmeURL)
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

	return nil
}

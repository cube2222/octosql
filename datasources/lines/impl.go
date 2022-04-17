package lines

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(ctx context.Context) (physical.Database, error) {
	return &Database{}, nil
}

type Database struct {
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (d *Database) GetTable(ctx context.Context, name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	info, err := os.Stat(name)
	if err != nil {
		return nil, physical.Schema{}, errors.Wrap(err, "couldn't check if file exists")
	}
	if info.IsDir() {
		return nil, physical.Schema{}, fmt.Errorf("%s is a directory", name)
	}

	separator := "\n"
	if sep, ok := options["sep"]; ok {
		separator = sep
	}

	return &impl{
			path:      name,
			separator: separator,
		},
		physical.NewSchema(
			[]physical.SchemaField{
				{
					Name: "number",
					Type: octosql.Int,
				},
				{
					Name: "text",
					Type: octosql.String,
				},
			},
			-1,
			physical.WithNoRetractions(true),
		),
		nil
}

type impl struct {
	path, separator string
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:      i.path,
		fields:    schema.Fields,
		separator: i.separator,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

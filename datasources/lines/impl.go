package lines

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/files"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	f, err := files.OpenLocalFile(context.Background(), name, files.WithPreview())
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't open local file: %w", err)
	}
	f.Close()

	separator := "\n"
	if sep, ok := options["sep"]; ok {
		separator = sep
	}

	return &impl{
			path:      name,
			separator: separator,
			tail:      options["tail"] == "true",
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
	tail            bool
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:      i.path,
		fields:    schema.Fields,
		separator: i.separator,
		tail:      i.tail,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

package json

import (
	"context"
	"os"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type JSONDataSourceDescription struct {
	path string
}

func NewJSONDataSourceDescription(path string) *JSONDataSourceDescription {
	return &JSONDataSourceDescription{
		path: path,
	}
}

func (dsd *JSONDataSourceDescription) Initialize(ctx context.Context, predicates octosql.Formula) (octosql.DataSource, error) {
	_, err := os.Stat(dsd.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't stat file")
	}

	return &JSONDataSource{
		path: dsd.path,
	}, nil
}

func (dsd *JSONDataSourceDescription) PrimaryKeys() map[string]struct{} {
	return make(map[string]struct{})
}

func (dsd *JSONDataSourceDescription) AvailableFilters() map[octosql.FieldType]map[octosql.RelationType]struct{} {
	return map[octosql.FieldType]map[octosql.RelationType]struct{}{
		octosql.Primary:   make(map[octosql.RelationType]struct{}),
		octosql.Secondary: make(map[octosql.RelationType]struct{}),
	}
}

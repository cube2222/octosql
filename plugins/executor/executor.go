package executor

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type Database struct {
}

func (d Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	panic("implement me")
}

package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type impl struct {
	config *Config
	schema physical.Schema
	table  string
}

func (i *impl) Schema() (physical.Schema, error) {
	return i.schema, nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment) (execution.Node, error) {
	// Prepare statement
	db, err := connect(i.config)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to database: %w", err)
	}
	fields := make([]string, len(i.schema.Fields))
	for index := range i.schema.Fields {
		fields[index] = i.schema.Fields[index].Name
	}
	stmt, err := db.PrepareContext(ctx, fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), i.table))
	if err != nil {
		return nil, fmt.Errorf("couldn't prepare statement: %w", err)
	}
	return &DatasourceExecuting{
		fields: i.schema.Fields,
		table:  i.table,
		db:     db,
		stmt:   stmt,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, pushedDown []physical.Expression, changed bool) {
	// TODO: Implement predicate pushdown for postgres
	return newPredicates, pushedDownPredicates, false
}
